from abc import ABC, abstractmethod
from enum import Enum, auto
from threading import Event
from typing import Sequence

import taro.client
from taro import ExecutionStateObserver, JobInfo
from taro.err import InvalidStateError
from taro.jobs.execution import ExecutionState
from taro.listening import StateReceiver


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
        """Dictionary of arbitrary immutable execution parameters"""
        return None

    def close(self):
        """
        Interrupt waiting and perform cleanup
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

    def __init__(self, syncs):
        self._syncs = tuple(syncs) if syncs else (NoSync(),)
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

    def close(self):
        for sync in self._syncs:
            sync.close()


class Latch(Sync):

    def __init__(self, waiting_state: ExecutionState):
        if not waiting_state.is_waiting():
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
        self.close()

    @property
    def parameters(self):
        return self._parameters

    def close(self):
        self._signal = Signal.CONTINUE
        self._event.set()


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

        jobs = taro.client.read_jobs_info()
        if any(j for j in jobs if j.id != job_info.id and j.matches(job_instance)):
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
            return ExecutionState.DEPENDENCY_NOT_RUNNING

        return ExecutionState.NONE

    def set_signal(self, job_info) -> Signal:
        jobs = taro.client.read_jobs_info()
        if any(j for j in jobs if any(j.matches(dependency) for dependency in self.dependencies)):
            self._signal = Signal.CONTINUE
        else:
            self._signal = Signal.TERMINATE

        return self._signal

    def wait_and_unlock(self, global_state_lock):
        raise InvalidStateError("Wait is not supported by no-overlap sync")

    @property
    def parameters(self):
        return self._parameters


class ParallelMax(Sync, ExecutionStateObserver):

    def __init__(self, max_executions, parallel_group):
        if max_executions < 1:
            raise ValueError('Max executions must be greater than zero')
        if not parallel_group:
            raise ValueError('Parallel group must be specified')
        self._max = max_executions
        self._parallel_group = parallel_group
        self._signal = Signal.NONE
        self._event = Event()
        self._parameters = (('sync', 'parallel_max'), ('parallel_group', parallel_group))

    @property
    def max_executions(self):
        return self._max

    @property
    def parallel_group(self):
        return self._parallel_group

    @property
    def current_signal(self) -> Signal:
        return self._signal

    @property
    def exec_state(self) -> ExecutionState:
        if self._signal is Signal.WAIT:
            return ExecutionState.WAITING

        return ExecutionState.NONE

    def set_signal(self, job_info) -> Signal:
        jobs = taro.client.read_jobs_info()
        parallel_group_jobs = sorted(
            (job for job in jobs if self._is_same_parallel_group(job)),
            key=lambda job: job.lifecycle.changed(ExecutionState.CREATED)
        )
        executing = [job for job in parallel_group_jobs if job.lifecycle.state.is_executing()]
        max_allowed = self.max_executions - len(executing)
        if max_allowed <= 0:
            return Signal.WAIT

        next_allowed = [job for job in parallel_group_jobs if job not in executing][0:max_allowed]
        job_created = job_info.lifecycle.changed(ExecutionState.CREATED)
        for allowed in next_allowed:
            if job_info.id == allowed.id or job_created <= allowed.lifecycle.changed(ExecutionState.CREATED):
                # The second condition ensure this works even when the job is not contained in 'job' for any reasons
                return Signal.CONTINUE

        return Signal.WAIT

    def _is_same_parallel_group(self, job):
        return any(1 for name, value in job.parameters if name == 'parallel_group' and value == self._parallel_group)

    def wait_and_unlock(self, global_state_lock):
        self._event.clear()
        global_state_lock.unlock()
        self._event.wait()

    @property
    def parameters(self):
        return self._parameters

    def close(self):
        self._event.set()

    def state_update(self, job_info: JobInfo):
        if job_info.lifecycle.state.is_terminal() and self._is_same_parallel_group(job_info):
            self._event.set()


class StateReceiverManagingSync(CompositeSync):
    def __init__(self, syncs, state_receiver_factory=StateReceiver):
        super().__init__(syncs)
        self._receiver = state_receiver_factory()
        for sync in syncs:
            self._receiver.listeners.append(sync)
        self._started = False

    def set_signal(self, job_info) -> Signal:
        if not self._started:
            self._receiver.start()
            self._started = True

        return super(StateReceiverManagingSync, self).set_signal(job_info)

    def close(self):
        self._receiver.close()
        for sync in self._syncs:
            self._receiver.listeners.remove(sync)

        super(StateReceiverManagingSync, self).close()


def create_composite(no_overlap: bool = False, depends_on: Sequence[str] = ()):
    syncs = []

    if no_overlap:
        syncs.append(NoOverlap())
    if depends_on:
        syncs.append(Dependency(*depends_on))

    return CompositeSync(syncs)