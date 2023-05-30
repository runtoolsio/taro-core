from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from threading import Event
from typing import Sequence

import taro.client
from taro.err import InvalidStateError
from taro.jobs.execution import ExecutionState, ExecutionPhase, Flag
from taro.jobs.job import JobInstanceMetadata, JobInfoList
from taro.listening import StateReceiver, ExecutionStateEventObserver
from taro.log import timing


class Signal(Enum):
    NONE = auto()
    """Initial state when signal is not yet set"""
    WAIT = auto()
    """Job must wait for a condition"""
    TERMINATE = auto()
    """Job must terminate due to a condition"""
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
    def wait_and_unlock(self, global_state_lock):
        """

        :param global_state_lock: global execution state lock for unlocking
        """

    @property
    def parameters(self):
        """Sequence of tuples representing arbitrary immutable sync parameters"""
        return None

    def release(self):
        """
        Interrupt waiting
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

    def wait_and_unlock(self, global_state_lock):
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

    def wait_and_unlock(self, global_state_lock):
        self._current.wait_and_unlock(global_state_lock)

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

    def wait_and_unlock(self, lock):
        if self._event.is_set():
            return

        lock.unlock()
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
        if self._signal is Signal.TERMINATE:
            return ExecutionState.SKIPPED

        return ExecutionState.NONE

    def set_signal(self, job_info) -> Signal:
        job_instance = self._job_instance or job_info.job_id

        jobs, _ = taro.client.read_jobs_info()
        if any(j for j in jobs if j.id != job_info.id and j.id.matches_pattern(job_instance)):
            self._signal = Signal.TERMINATE
        else:
            self._signal = Signal.CONTINUE

        return self.current_signal

    def wait_and_unlock(self, global_state_lock):
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
        if self._signal is Signal.TERMINATE:
            return ExecutionState.UNSATISFIED

        return ExecutionState.NONE

    def set_signal(self, job_info) -> Signal:
        jobs, _ = taro.client.read_jobs_info()
        if any(j for j in jobs if any(j.id.matches_pattern(dependency) for dependency in self.dependencies)):
            self._signal = Signal.CONTINUE
        else:
            self._signal = Signal.TERMINATE

        return self._signal

    def wait_and_unlock(self, global_state_lock):
        raise InvalidStateError("Wait is not supported by no-overlap sync")

    @property
    def parameters(self):
        return self._parameters


class ExecutionsLimitation(Sync, ExecutionStateEventObserver):

    def __init__(self, execution_group, max_executions):
        if not execution_group:
            raise ValueError('Execution group must be specified')
        if max_executions < 1:
            raise ValueError('Max executions must be greater than zero')
        self._group = execution_group
        self._max = max_executions
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
    def set_signal(self, job_info) -> Signal:
        jobs, _ = taro.client.read_jobs_info()

        exec_group_jobs_sorted = JobInfoList(sorted(
            (job for job in jobs if self._is_same_exec_group(job.metadata)),
            key=lambda job: job.lifecycle.changed_at(ExecutionState.CREATED)
        ))
        more_allowed = self.max_executions - len(exec_group_jobs_sorted.executing)
        if more_allowed <= 0:
            return self._set_signal(Signal.WAIT)

        next_allowed = exec_group_jobs_sorted.scheduled[0:more_allowed] # TODO Important This doesn't look correct - should iterate only thru queued
        job_created = job_info.lifecycle.changed_at(ExecutionState.CREATED)
        for allowed in next_allowed:
            if job_info.id == allowed.id or job_created <= allowed.lifecycle.changed_at(ExecutionState.CREATED):
                # The second condition ensure this works even when the job is not contained in 'job' for any reasons
                return self._set_signal(Signal.CONTINUE)

        return self._set_signal(Signal.WAIT)

    def _is_same_exec_group(self, instance_meta):
        return instance_meta.id.job_id == self._group or \
               any(1 for name, value in instance_meta.parameters if name == 'execution_group' and value == self._group)

    def wait_and_unlock(self, global_state_lock):
        self._event.clear()
        global_state_lock.unlock()
        self._event.wait()

    @property
    def parameters(self):
        return self._parameters

    def release(self):
        self._event.set()

    def state_update(self, instance_meta: JobInstanceMetadata, previous_state, new_state, changed):
        if new_state.in_phase(ExecutionPhase.TERMINAL) and self._is_same_exec_group(instance_meta):
            self._event.set()


class WaitForStateWrapper(CompositeSync):
    def __init__(self, *syncs, state_receiver_factory=StateReceiver):
        super().__init__(*syncs)
        self._state_receiver_factory = state_receiver_factory

    def wait_and_unlock(self, global_state_lock):
        receiver = self._state_receiver_factory()
        receiver.listeners.append(self._current)
        receiver.start()
        try:
            super(WaitForStateWrapper, self).wait_and_unlock(global_state_lock)
        finally:
            receiver.close()
            receiver.listeners.remove(self._current)


@dataclass
class ExecutionsLimit:
    execution_group: str
    max_executions: int


def create_composite(executions_limit: ExecutionsLimit = None, no_overlap: bool = False, depends_on: Sequence[str] = ()):
    syncs = []

    if executions_limit:
        syncs.append(WaitForStateWrapper(
            ExecutionsLimitation(executions_limit.execution_group, executions_limit.max_executions)))
    if no_overlap:
        syncs.append(NoOverlap())
    if depends_on:
        syncs.append(Dependency(*depends_on))

    return CompositeSync(*syncs)
