from abc import ABC, abstractmethod
from threading import Lock

from tarotools.taro import TerminationStatus


class PhaseAction(ABC):

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

    def __init__(self, init_phase, inter_phases, term_phase):
        self._init_phase = init_phase
        self._inter_phases = inter_phases
        self._term_phase = term_phase
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
        term_status = None
        for phase in (self._inter_phases + [self._term_phase]):
            with self._phase_lock:
                if self._abort:
                    return
                if term_status and not self._term_status:
                    self._term_status = term_status
                if self._term_status:
                    phase = self._term_phase

                self.next_phase(phase)

            term_status = phase.execute()
            if phase == self._term_phase:
                return

    def next_phase(self, phase):
        self._current_phase = phase
        # notify listeners

    def stop(self):
        run_term = False
        with self._phase_lock:
            if self._term_status:
                return

            self._term_status = self._current_phase.stop_status
            assert self._term_status
            if self._current_phase == self._init_phase:
                # Not started yet
                # Prevent phase transition...
                self._abort = True
                # ...and run term manually
                run_term = True
                self.next_phase(self._term_phase)

        self._current_phase.stop()  # Can be changed to term phase meanwhile, but term phase stop is no-ops

        if run_term:
            self._term_phase.execute()
