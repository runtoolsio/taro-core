import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from threading import Event
from typing import Sequence

from tarotools import taro
from tarotools.taro.err import InvalidStateError
from tarotools.taro.jobs.execution import ExecutionState, ExecutionPhase, Flag
from tarotools.taro.jobs.inst import JobInstanceMetadata, JobInstances
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

    @property
    @abstractmethod
    def exec_state(self) -> ExecutionState:
        """
        TODO return in tuple in set_signal?
        :return: execution state for the current signal or NONE state
        """

    @abstractmethod
    def set_signal(self, job_info) -> Signal:
        """
        If returned signal is 'WAIT' then the job is obligated to call :func:`wait_and_release`
        which will likely suspend the job until an awaited condition is changed.

        :param job_info:
        :param: job_info job for which the signal is being set
        :return: sync state for job
        """

    @abstractmethod
    def wait(self):
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

    @property
    def exec_state(self) -> ExecutionState:
        return ExecutionState.NONE

    def set_signal(self, job_info) -> Signal:
        self._current_signal = Signal.CONTINUE
        return self.current_signal

    def wait(self):
        raise InvalidStateError('Wait is not supported and this method is not supposed to be called')


class CompositeSync(Sync):

    def __init__(self, *syncs):
        self._syncs = syncs or (NoSync(),)
        self._current = self._syncs[0]
        self._parameters = tuple(p for s in syncs if s.parameters for p in s.parameters)

    @property
    def current_signal(self) -> Signal:
        return self._current.current_signal

    @property
    def exec_state(self) -> ExecutionState:
        return self._current.exec_state

    def set_signal(self, job_info) -> Signal:
        for sync in self._syncs:
            self._current = sync
            signal = sync.set_signal(job_info)
            if signal is not Signal.CONTINUE:
                break

        return self.current_signal

    def wait(self):
        self._current.wait()

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

    @property
    def exec_state(self) -> ExecutionState:
        if self._signal is Signal.WAIT:
            return self.waiting_state

        return ExecutionState.NONE

    def set_signal(self, job_info) -> Signal:
        if self._event.is_set():
            self._signal = Signal.CONTINUE
        else:
            self._signal = Signal.WAIT

        return self._signal

    def wait(self):
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

    @property
    def exec_state(self) -> ExecutionState:
        if self._signal is Signal.REJECT:
            return ExecutionState.SKIPPED

        return ExecutionState.NONE

    def set_signal(self, job_info) -> Signal:
        job_instance = self._job_instance or job_info.job_id

        jobs, _ = taro.client.read_instances()
        if any(j for j in jobs if j.id != job_info.id and j.id.matches_pattern(job_instance)):
            self._signal = Signal.REJECT
        else:
            self._signal = Signal.CONTINUE

        return self.current_signal

    def wait(self):
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

    @property
    def exec_state(self) -> ExecutionState:
        if self._signal is Signal.REJECT:
            return ExecutionState.UNSATISFIED

        return ExecutionState.NONE

    def set_signal(self, job_info) -> Signal:
        jobs, _ = taro.client.read_instances()
        if any(j for j in jobs if any(j.id.matches_pattern(dependency) for dependency in self.dependencies)):
            self._signal = Signal.CONTINUE
        else:
            self._signal = Signal.REJECT

        return self._signal

    def wait(self):
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
        self._state_receiver = None
        self._signal = Signal.NONE
        self._event = Event()
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

    @property
    def exec_state(self) -> ExecutionState:
        if self._signal is Signal.WAIT:
            return ExecutionState.QUEUED

        return ExecutionState.NONE

    @timing('exec_limit_set_signal', args_idx=[1])
    def set_signal(self, job_inst) -> Signal:
        # We must clear the signal first to not miss any updates after `read_instances()` is called
        self._event.clear()

        if not self._state_receiver:
            self._state_receiver = self._state_receiver_factory()
            self._state_receiver.listeners.append(self)
            self._state_receiver.start()

        jobs, _ = taro.client.read_instances()

        group_jobs_sorted = JobInstances(sorted(
            (job for job in jobs if self._is_same_exec_group(job.metadata)),
            key=lambda job: job.lifecycle.changed_at(ExecutionState.CREATED)
        ))
        allowed_count = self.max_executions - len(group_jobs_sorted.executing)
        if allowed_count <= 0:
            return self._set_signal(Signal.WAIT)

        next_allowed = group_jobs_sorted.scheduled[0:allowed_count]
        job_created = job_inst.lifecycle.changed_at(ExecutionState.CREATED)
        for allowed in next_allowed:
            if job_inst.id == allowed.id or job_created <= allowed.lifecycle.changed_at(ExecutionState.CREATED):
                # The second condition ensure this works even when the job is not contained in the list for any reasons
                return self._set_signal(Signal.CONTINUE)

        return self._set_signal(Signal.WAIT)

    def _is_same_exec_group(self, instance_meta):
        return instance_meta.id.job_id == self._group or \
            any(1 for name, value in instance_meta.parameters if name == 'execution_group' and value == self._group)

    def wait(self):
        log.debug("event=[exec_limit_coord_wait_starting]")
        set_ = self._event.wait(60)

        if set_:
            log.debug("event=[exec_limit_coord_wait_finished]")
        else:
            log.warning("event=[exec_limit_coord_timeout]")

    @property
    def parameters(self):
        return self._parameters

    def release(self):
        self._event.set()

    def state_update(self, instance_meta: JobInstanceMetadata, previous_state, new_state, changed):
        if new_state.in_phase(ExecutionPhase.TERMINAL) and self._is_same_exec_group(instance_meta):
            self._event.set()

    def close(self):
        if self._state_receiver:
            self._state_receiver.close()
            self._state_receiver.listeners.remove(self)


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
