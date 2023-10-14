import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from threading import Event, Condition, Lock
from typing import Sequence, Optional
from weakref import WeakKeyDictionary

from tarotools import taro
from tarotools.taro.err import InvalidStateError
from tarotools.taro.jobs.execution import ExecutionState, ExecutionPhase, Flag
from tarotools.taro.jobs.inst import JobInstances, InstanceMatchCriteria, IDMatchCriteria
from tarotools.taro.listening import StateReceiver, ExecutionStateEventObserver
from tarotools.taro.log import timing

log = logging.getLogger(__name__)


class WaitCondition:
    LATCH = "LATCH"
    EXEC_GROUP = "EXEC_GROUP"


class Directive(ABC):
    pass


class Continue(Directive):
    # TBA
    pass


CONTINUE = Continue()


class Wait(Directive):
    def __init__(self, state: ExecutionState, wait_condition: str = None):
        self._wait_condition = wait_condition
        if not state.has_flag(Flag.WAITING):
            raise ValueError("Not a waiting state as expected by wait directive: " + str(state))
        self.state = state

    @property
    def wait_condition(self) -> Optional[str]:
        return self._wait_condition

    @abstractmethod
    def wait(self):
        """To be implemented by subclasses."""
        pass

    @abstractmethod
    def release(self):
        """
        Interrupt waiting
        """
        pass


class Reject(Directive):
    def __init__(self, state: ExecutionState):
        if not state.has_flag(Flag.REJECTED):
            raise ValueError("Not a rejected state as expected by reject directive: " + str(state))
        self.state = state


class Coordination(ABC):

    @property
    def parameters(self):
        """Sequence of tuples representing arbitrary immutable sync parameters"""
        return None

    @abstractmethod
    def coordinate(self, instance) -> Directive:
        """
        If returned signal is 'WAIT' then the job is obligated to call :func:`wait_and_release`
        which will likely suspend the job until an awaited condition is changed.

        :param instance:
        :param: job_info job for which the signal is being set
        :return: sync state for job
        """


class NoCoordination(Coordination):

    def coordinate(self, instance) -> Directive:
        return CONTINUE


class CompositeCoord(Coordination):

    def __init__(self, *coords):
        self._coords = list(coords) or (NoCoordination(),)
        self._parameters = tuple(p for c in coords if c.parameters for p in c.parameters)
        self._current_lock = Lock()  # Lock for the fields below
        self._current_coord = None
        self._current_directive = None
        self._released = False

    @property
    def parameters(self):
        return self._parameters

    def coordinate(self, instance) -> Directive:
        with self._current_lock:
            if self._released:
                return CONTINUE
            for coord in self._coords:
                self._current_coord = coord
                self._current_directive = coord.coordinate(instance)
                if not isinstance(self._current_directive, Continue):
                    break

        return self._current_directive

    def wait(self):
        if not isinstance(self._current_directive, Wait):
            raise InvalidStateError("Cannot wait on current directive: " + str(self._current_directive))
        if not self._released:
            self._current_directive.wait()

    def remove_wait(self, predicate):
        with self._current_lock:
            cur_dir = self._current_directive
            if isinstance(cur_dir, Wait) and predicate(cur_dir):
                self._coords.remove(self._current_coord)
                self._current_coord = self._current_directive = None
                cur_dir.release()

    def release(self):
        with self._current_lock:
            self._released = True
            if isinstance(self._current_directive, Wait):
                self._current_directive.release()


class Latch(Coordination):

    def __init__(self, waiting_state: ExecutionState):
        self._waiting_state = waiting_state
        self._event = Event()
        self._release_lock = Lock()
        self._released = False
        self._parameters = (('coordination', 'latch'), ('latch_waiting_state', str(waiting_state)))
        self._wait_directive = Latch._Wait(self)

    class _Wait(Wait):

        def __init__(self, latch: "Latch"):
            super().__init__(latch._waiting_state, WaitCondition.LATCH)
            self.latch = latch

        def wait(self):
            if self.latch._released:
                return

            self.latch._event.wait()

        def release(self):
            # Released flag not set, it is expected that only the instance calling this method will be released
            self.latch._event.set()

    @property
    def parameters(self):
        return self._parameters

    @property
    def released(self):
        return self._released

    def coordinate(self, instance) -> Directive:
        if self._event.is_set():
            with self._release_lock:
                if self._released:
                    return CONTINUE
                else:
                    self._event.clear()

        return self._wait_directive

    def release(self):
        with self._release_lock:
            self._released = True
            self._event.set()


class NoOverlap(Coordination):

    def __init__(self, instance_match=None):
        self._instance_match = instance_match
        no_overlap = str(instance_match.to_dict(False)) if instance_match else 'same_job_id'
        self._parameters = (('coordination', 'no_overlap'), ('no_overlap', no_overlap))

    @property
    def instance_match(self):
        return self._instance_match

    @property
    def parameters(self):
        return self._parameters

    def coordinate(self, instance) -> Directive:
        inst_match = self._instance_match or InstanceMatchCriteria(IDMatchCriteria(instance.job_id))

        instances, _ = taro.client.read_instances()
        if any(i for i in instances if i.id != instance.id and inst_match.matches(i)):
            return Reject(ExecutionState.SKIPPED)

        return CONTINUE


class Dependency(Coordination):

    def __init__(self, dependency_match):
        self._dependency_match = dependency_match
        self._parameters = (('sync', 'dependency'), ('dependency', str(dependency_match.to_dict(False))))

    @property
    def dependency_match(self):
        return self._dependency_match

    @property
    def parameters(self):
        return self._parameters

    def coordinate(self, instance) -> Directive:
        instances, _ = taro.client.read_instances()
        if not any(i for i in instances if self._dependency_match.matches(i)):
            return Reject(ExecutionState.UNSATISFIED)

        return CONTINUE


class ExecutionsLimitation(Coordination, ExecutionStateEventObserver):

    def __init__(self, execution_group, max_executions, state_receiver_factory=StateReceiver):
        if not execution_group:
            raise ValueError('Execution group must be specified')
        if max_executions < 1:
            raise ValueError('Max executions must be greater than zero')
        self._group = execution_group
        self._max = max_executions
        self._state_receiver_factory = state_receiver_factory
        self._inst_to_wait_id = WeakKeyDictionary()

        self._wait_guard = Condition()
        # vv Guarding these fields vv
        self._wait_counter = 0
        self._current_wait = None
        self._state_receiver = None

        self._parameters = (
            ('sync', 'executions_limitation'),
            ('execution_group', execution_group),
            ('max_executions', max_executions)
        )

    class _Wait(Wait):

        def __init__(self, outer: "ExecutionsLimitation", wait_id):
            super().__init__(ExecutionState.QUEUED, WaitCondition.EXEC_GROUP)
            self.outer = outer
            self.wait_id = wait_id

        def wait(self):
            log.debug("event=[exec_limit_coord_wait_starting]")
            self.outer._wait(self.wait_id)
            log.debug("event=[exec_limit_coord_wait_finished]")

        def release(self):
            self.outer._release()

    @property
    def execution_group(self):
        return self._group

    @property
    def max_executions(self):
        return self._max

    @property
    def parameters(self):
        return self._parameters

    def _allowed_continue(self, job_inst) -> bool:
        jobs, _ = taro.client.read_instances()

        group_jobs_sorted = JobInstances(sorted(
            (job for job in jobs if self._is_same_exec_group(job.metadata)),
            key=lambda job: job.lifecycle.changed_at(ExecutionState.CREATED)
        ))
        allowed_count = self.max_executions - len(group_jobs_sorted.executing)
        if allowed_count <= 0:
            return False

        next_allowed = group_jobs_sorted.scheduled[0:allowed_count]
        job_created = job_inst.lifecycle.changed_at(ExecutionState.CREATED)
        for allowed in next_allowed:
            if job_inst.id == allowed.id or job_created <= allowed.lifecycle.changed_at(ExecutionState.CREATED):
                # The second condition ensure this works even when the job is not contained in the list for any reasons
                return True

        return False

    def _is_same_exec_group(self, instance_meta):
        return instance_meta.id.job_id == self._group or \
            any(1 for name, value in instance_meta.parameters if name == 'execution_group' and value == self._group)

    @timing('exec_limit_set_signal', args_idx=[1])
    def coordinate(self, instance) -> Directive:
        if self._allowed_continue(instance):
            return CONTINUE

        with self._wait_guard:
            current_wait = self._current_wait
            if not current_wait:
                current_wait = self._setup_waiting()
                if self._allowed_continue(instance):
                    self._remove_waiting()
                    return CONTINUE

        return self._Wait(self, current_wait)

    def _setup_waiting(self):
        self._start_listening()

        self._wait_counter += 1
        self._current_wait = self._wait_counter
        return self._current_wait

    def _start_listening(self):
        self._state_receiver = self._state_receiver_factory()
        self._state_receiver.listeners.append(self)
        self._state_receiver.start()

    def state_update(self, instance_meta, _, new_state, __):
        with self._wait_guard:
            if not self._current_wait:
                return
            if new_state.in_phase(ExecutionPhase.TERMINAL) and self._is_same_exec_group(instance_meta):
                self._remove_waiting()
                self._wait_guard.notify_all()

    def _remove_waiting(self):
        self._current_wait = None
        self._stop_listening()

    def _stop_listening(self):
        self._state_receiver.close()
        self._state_receiver.listeners.remove(self)
        self._state_receiver = None

    def _wait(self, wait_id):
        log.debug("event=[exec_limit_coord_wait_starting]")

        with self._wait_guard:
            if not self._current_wait or self._current_wait != wait_id:
                return
            self._wait_guard.wait()

        log.debug("event=[exec_limit_coord_wait_finished]")

    def _release(self):
        with self._wait_guard:
            self._remove_waiting()
            self._wait_guard.notify_all()


@dataclass
class ExecutionsLimit:
    execution_group: str
    max_executions: int


# TODO delete
def create_composite(executions_limit: ExecutionsLimit = None, no_overlap: bool = False,
                     depends_on: Sequence[str] = ()):
    syncs = []

    if executions_limit:
        limitation = ExecutionsLimitation(executions_limit.execution_group, executions_limit.max_executions)
        syncs.append(limitation)
    if no_overlap:
        syncs.append(NoOverlap())
    if depends_on:
        syncs.append(Dependency(*depends_on))

    return syncs
