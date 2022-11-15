from abc import ABC, abstractmethod
from threading import Event

from taro import ExecutionState


class Sync(ABC):

    @property
    @abstractmethod
    def state(self) -> ExecutionState:
        """Return current state"""

    @abstractmethod
    def set_state(self) -> ExecutionState:
        """
        If 'NONE' state is returned then the job can proceed with the execution.
        If returned state belongs to the 'TERMINAL' group then the job must be terminated.
        If returned state belongs to the 'WAITING' group then the job is obligated to call :func:`wait_and_release`.

        :return: execution state for job
        """

    @abstractmethod
    def wait_and_unlock(self, global_state_lock):
        """

        :param global_state_lock: global execution state lock for unlocking
        """

    @abstractmethod
    def release(self):
        """
        Interrupt waiting
        """


class NoSync(Sync):
    @property
    def state(self) -> ExecutionState:
        return ExecutionState.NONE

    def set_state(self) -> ExecutionState:
        return ExecutionState.NONE

    def wait_and_unlock(self, global_state_lock):
        pass

    def release(self):
        pass


class CompositeSync(Sync):

    def __init__(self, syncs):
        self._state = ExecutionState.NONE
        self._syncs = list(syncs)
        self._current = NoSync()

    @property
    def state(self) -> ExecutionState:
        return self._current.state

    def set_state(self) -> ExecutionState:
        self._current = NoSync()

        for sync in self._syncs:
            state = sync.set_state()
            if state is not ExecutionState.NONE:
                self._current = sync

        return self.state

    def wait_and_unlock(self, global_state_lock):
        self._current.wait_and_unlock(global_state_lock)

    def release(self):
        self._current.release()


class Latch(Sync):

    def __init__(self, waiting_state: ExecutionState):
        if not waiting_state.is_waiting():
            raise ValueError(f"Invalid execution state for latch: {waiting_state}. Latch requires waiting state.")
        self._state = ExecutionState.NONE
        self._event = Event()
        self.waiting_state = waiting_state

    @property
    def state(self) -> ExecutionState:
        return self._state

    def set_state(self) -> ExecutionState:
        if self._event.is_set():
            self._state = ExecutionState.NONE
        else:
            self._state = self.waiting_state

        return self._state

    def wait_and_unlock(self, lock):
        if self._event.is_set():
            return

        lock.unlock()
        self._event.wait()

    def release(self):
        self._event.set()
