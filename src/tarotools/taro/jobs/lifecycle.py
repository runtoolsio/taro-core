from abc import ABC, abstractmethod
from threading import Lock

from tarotools.taro import TerminationStatus
from tarotools.taro.jobs.instance import InstancePhase


class PhaseAction(ABC):
    """
    TODO:
    1. Preconditions
    """

    @property
    @abstractmethod
    def phase(self):
        pass

    @abstractmethod
    def execute(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @property
    @abstractmethod
    def stop_status(self):
        pass


class Phaser:

    def __init__(self, lifecycle, init_phase, term_phase, phases):
        self._lifecycle = lifecycle
        self._init_phase = init_phase
        self._term_phase = term_phase
        self._phases = phases
        self._phase_lock = Lock()

        # Guarded by the lock:
        self._abort = False
        self._current_phase = None
        self._term_status = TerminationStatus.NONE

    def prime(self):
        """
        TODO Impl
        """
        pass

    def run(self):
        # TODO prime check
        term_status = None
        for phase in self._phases:
            with self._phase_lock:
                if self._abort:
                    return
                if term_status and not self._term_status:
                    self._term_status = term_status
                if self._term_status:
                    self.next_phase(self._term_phase)
                    return

                self.next_phase(phase)

            term_status = phase.execute()

        self.next_phase(self._term_phase)

    def next_phase(self, phase):
        self._current_phase = phase
        # notify listeners

    def stop(self):
        with self._phase_lock:
            if self._term_status:
                return

            self._term_status = self._current_phase.stop_status
            assert self._term_status
            if self._current_phase == self._init_phase:
                # Not started yet
                self._abort = True  # Prevent phase transition...
                self.next_phase(self._term_phase)

        self._current_phase.stop()


class PendingPhase(PhaseAction):

    def __init__(self, waiters):
        self._waiters = waiters

    @property
    def phase(self):
        return InstancePhase.PENDING

    def execute(self):
        for waiter in self._waiters:
            waiter.wait()
        return TerminationStatus.NONE

    def stop(self):
        for waiter in self._waiters:
            waiter.cancel()

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED


class QueuePhase(PhaseAction):

    def __init__(self, queue_waiter):
        self._queue_waiter = queue_waiter

    @property
    def phase(self):
        return InstancePhase.QUEUED

    def execute(self):
        return self._queue_waiter.wait()

    def stop(self):
        self._queue_waiter.cancel()

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED
