from abc import ABC, abstractmethod
from enum import Enum, auto
from threading import Event

from taro import ExecutionState


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
    def set_signal(self) -> Signal:
        """
        If returned signal is 'WAIT' then the job is obligated to call :func:`wait_and_release`
        which will likely suspend the job until an awaited condition is changed.

        :return: sync state for job
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

    def __init__(self):
        self._current_signal = Signal.NONE

    @property
    def current_signal(self) -> Signal:
        return self._current_signal

    @property
    def exec_state(self) -> ExecutionState:
        return ExecutionState.NONE

    def set_signal(self) -> Signal:
        self._current_signal = Signal.CONTINUE
        return self.current_signal

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
    def current_signal(self) -> Signal:
        return self._current.current_signal

    @property
    def exec_state(self) -> ExecutionState:
        return self._current.exec_state

    def set_signal(self) -> Signal:
        for sync in self._syncs:
            self._current = sync
            signal = sync.set_signal()
            if signal is not Signal.CONTINUE:
                break

        return self.current_signal

    def wait_and_unlock(self, global_state_lock):
        self._current.wait_and_unlock(global_state_lock)

    def release(self):
        self._current.release()


class Latch(Sync):

    def __init__(self, waiting_state: ExecutionState):
        if not waiting_state.is_waiting():
            raise ValueError(f"Invalid execution state for latch: {waiting_state}. Latch requires waiting state.")
        self._signal = Signal.NONE
        self._event = Event()
        self.waiting_state = waiting_state

    @property
    def current_signal(self) -> Signal:
        return self._signal

    @property
    def exec_state(self) -> ExecutionState:
        if self._signal is Signal.WAIT:
            return self.waiting_state

        return ExecutionState.NONE

    def set_signal(self) -> Signal:
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
        self._signal = Signal.CONTINUE

    def release(self):
        self._event.set()
