from abc import ABC, abstractmethod
from threading import Event

from taro import ExecutionState


class Sync(ABC):

    @abstractmethod
    def new_state(self) -> ExecutionState:
        """
        If 'NONE' state is returned then the job can proceed with the execution.
        If returned state belongs to the 'TERMINAL' group then the job must be terminated.
        If returned state belongs to the 'WAITING' group then the job is obligated to call :func:`wait_and_release`.

        :return: execution state for job
        """

    @abstractmethod
    def wait_and_unlock(self, unlock):
        """

        :param unlock: function unlocking global execution state lock
        """


class NoSync(Sync):

    def new_state(self) -> ExecutionState:
        return ExecutionState.NONE

    def wait_and_unlock(self, state_lock):
        pass


class Latch(Sync):

    def __init__(self, waiting_state: ExecutionState):
        if not waiting_state.is_waiting():
            raise ValueError(f"Invalid execution state for latch: {waiting_state}. Latch requires waiting state.")
        self._event = Event()
        self.waiting_state = waiting_state

    def new_state(self) -> ExecutionState:
        if self._event.is_set():
            return ExecutionState.NONE

        return self.waiting_state

    def release(self):
        self._event.set()

    def wait_and_unlock(self, lock):
        if self._event.is_set():
            return

        lock.unlock()
        self._event.wait()
