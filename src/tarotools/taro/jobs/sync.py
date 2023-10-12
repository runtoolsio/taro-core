import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from threading import Event, Condition
from typing import Sequence
from weakref import WeakKeyDictionary

from tarotools import taro
from tarotools.taro.err import InvalidStateError
from tarotools.taro.jobs.execution import ExecutionState, ExecutionPhase, Flag
from tarotools.taro.jobs.inst import JobInstances
from tarotools.taro.listening import StateReceiver, ExecutionStateEventObserver
from tarotools.taro.log import timing

log = logging.getLogger(__name__)


class Signal(Enum):
    NONE = auto()
    """Initial state when signal is not yet set"""
    WAIT = auto()
    """Job must wait for a condition"""
    REJECT = auto()
    """Job must be rejected terminate due to not met condition"""
    CONTINUE = auto()
    """Job is free to proceed with its execution"""


class Sync(ABC):

    @property
    @abstractmethod
    def current_signal(self) -> Signal:
        """
        :return: currently set signal on this object or NONE signal if the signal is not yet set
        """

    @abstractmethod
    def exec_state(self, signal) -> ExecutionState:
        """
        TODO return in tuple in set_signal?
        :return: execution state for the current signal or NONE state
        """

    @abstractmethod
    def set_signal(self, job_inst) -> Signal:
        """
        If returned signal is 'WAIT' then the job is obligated to call :func:`wait_and_release`
        which will likely suspend the job until an awaited condition is changed.

        :param job_inst:
        :param: job_info job for which the signal is being set
        :return: sync state for job
        """

    @abstractmethod
    def wait(self, job_inst):
        """

        """

    @property
    def parameters(self):
        """Sequence of tuples representing arbitrary immutable sync parameters"""
        return None

    def release(self):
        """
        Interrupt waiting
        """

    def close(self):
        """
        Close resources if needed
        """


class NoSync(Sync):

    def __init__(self):
        self._current_signal = Signal.NONE

    @property
    def current_signal(self) -> Signal:
        return self._current_signal

    def exec_state(self, signal) -> ExecutionState:
        return ExecutionState.NONE

    def set_signal(self, job_inst) -> Signal:
        self._current_signal = Signal.CONTINUE
        return self.current_signal

    def wait(self, job_inst):
        raise InvalidStateError('Wait is not supported and this method is not supposed to be called')


class CompositeSync(Sync):

    def __init__(self, *syncs):
        self._syncs = syncs or (NoSync(),)
        self._current = self._syncs[0]
        self._parameters = tuple(p for s in syncs if s.parameters for p in s.parameters)

    @property
    def current_signal(self) -> Signal:
        return self._current.current_signal

    def exec_state(self, signal) -> ExecutionState:
        return self._current.exec_state(signal)

    def set_signal(self, job_inst) -> Signal:
        for sync in self._syncs:
            self._current = sync
            signal = sync.set_signal(job_inst)
            if signal is not Signal.CONTINUE:
                break

        return self.current_signal

    def wait(self, job_inst):
        self._current.wait(job_inst)

    @property
    def parameters(self):
        return self._parameters

    def release(self):
        for sync in self._syncs:
            sync.release()


class Latch(Sync):

    def __init__(self, waiting_state: ExecutionState):
        if not waiting_state.has_flag(Flag.WAITING):
            raise ValueError(f"Invalid execution state for latch: {waiting_state}. Latch requires waiting state.")
        self._signal = Signal.NONE
        self._event = Event()
        self._parameters = (('sync', 'latch'), ('latch_waiting_state', str(waiting_state)))
        self.waiting_state = waiting_state

    @property
    def current_signal(self) -> Signal:
        return self._signal

    def exec_state(self, signal) -> ExecutionState:
        if self._signal is Signal.WAIT:
            return self.waiting_state

        return ExecutionState.NONE

    def set_signal(self, job_inst) -> Signal:
        if self._event.is_set():
            self._signal = Signal.CONTINUE
        else:
            self._signal = Signal.WAIT

        return self._signal

    def wait(self, job_inst):
        if self._event.is_set():
            return

        self._event.wait()

    def release(self):
        self._signal = Signal.CONTINUE
        self._event.set()

    @property
    def parameters(self):
        return self._parameters


class NoOverlap(Sync):

    def __init__(self, job_instance=None):
        self._job_instance = job_instance
        self._signal = Signal.NONE
        self._parameters = (('sync', 'no_overlap'), ('no_overlap', self._job_instance))

    @property
    def job_instance(self):
        return self._job_instance

    @property
    def current_signal(self) -> Signal:
        return self._signal

    def exec_state(self, signal) -> ExecutionState:
        if self._signal is Signal.REJECT:
            return ExecutionState.SKIPPED

        return ExecutionState.NONE

    def set_signal(self, job_inst) -> Signal:
        job_instance = self._job_instance or job_inst.job_id

        jobs, _ = taro.client.read_instances()
        if any(j for j in jobs if j.id != job_inst.id and j.id.matches_pattern(job_instance)):
            self._signal = Signal.REJECT
        else:
            self._signal = Signal.CONTINUE

        return self.current_signal

    def wait(self, job_inst):
        raise InvalidStateError("Wait is not supported by no-overlap sync")

    @property
    def parameters(self):
        return self._parameters


class Dependency(Sync):

    def __init__(self, *dependencies):
        self.dependencies = dependencies
        self._signal = Signal.NONE
        self._parameters = (('sync', 'dependency'), ('dependencies', ",".join(dependencies)))

    @property
    def current_signal(self) -> Signal:
        return self._signal

    def exec_state(self, signal) -> ExecutionState:
        if self._signal is Signal.REJECT:
            return ExecutionState.UNSATISFIED

        return ExecutionState.NONE

    def set_signal(self, job_inst) -> Signal:
        jobs, _ = taro.client.read_instances()
        if any(j for j in jobs if any(j.id.matches_pattern(dependency) for dependency in self.dependencies)):
            self._signal = Signal.CONTINUE
        else:
            self._signal = Signal.REJECT

        return self._signal

    def wait(self, job_inst):
        raise InvalidStateError("Wait is not supported by no-overlap sync")

    @property
    def parameters(self):
        return self._parameters


class ExecutionsLimitation(Sync, ExecutionStateEventObserver):

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

        self._signal = Signal.NONE

        self._parameters = (
            ('sync', 'executions_limitation'),
            ('execution_group', execution_group),
            ('max_executions', max_executions)
        )

    @property
    def group(self):
        return self._group

    @property
    def max_executions(self):
        return self._max

    @property
    def current_signal(self) -> Signal:
        return self._signal

    def _set_signal(self, signal):
        self._signal = signal
        return signal

    def exec_state(self, signal) -> ExecutionState:
        if signal is Signal.WAIT:
            return ExecutionState.QUEUED

        return ExecutionState.NONE

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
    def set_signal(self, job_inst) -> Signal:
        if self._allowed_continue(job_inst):
            return Signal.CONTINUE

        with self._wait_guard:
            if not self._current_wait:
                self._current_wait = self._setup_waiting()
                if self._allowed_continue(job_inst):
                    self._remove_waiting()
                    return Signal.CONTINUE

            self._inst_to_wait_id[job_inst] = self._current_wait

        return Signal.WAIT

    def _setup_waiting(self):
        self._start_listening()
        self._wait_counter += 1
        return self._wait_counter

    def _start_listening(self):
        self._state_receiver = self._state_receiver_factory()
        self._state_receiver.listeners.append(self)
        self._state_receiver.start()

    def wait(self, job_inst):
        log.debug("event=[exec_limit_coord_wait_starting]")

        wait_id = self._inst_to_wait_id.pop(job_inst)
        with self._wait_guard:
            if not self._current_wait or self._current_wait != wait_id:
                return
            self._wait_guard.wait()

        log.debug("event=[exec_limit_coord_wait_finished]")

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

    @property
    def parameters(self):
        return self._parameters

    def release(self):
        with self._wait_guard:
            self._remove_waiting()
            self._wait_guard.notify_all()


@dataclass
class ExecutionsLimit:
    execution_group: str
    max_executions: int


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

    return CompositeSync(*syncs)
