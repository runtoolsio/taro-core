import datetime
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum, auto
from threading import Lock
from typing import Tuple, Optional, List, Iterable, Dict, Any

from tarotools.taro import TerminationStatus, util
from tarotools.taro.jobs.instance import Phase
from tarotools.taro.util import format_dt_iso, is_empty


class InstanceState(Enum):
    NONE = auto()
    UNKNOWN = auto()
    CREATED = auto()
    PENDING = auto()
    AWAITING_APPROVAL = auto()
    IN_QUEUE = auto()
    EXECUTING = auto()
    ENDED = auto()

    @classmethod
    def from_str(cls, value: str):
        try:
            return cls[value.upper()]
        except KeyError:
            return cls.UNKNOWN


@dataclass(frozen=True)
class Phase:
    name: str
    state: InstanceState

    @classmethod
    def from_dict(cls, as_dict):
        cls(as_dict['name'], InstanceState.from_str(as_dict['state']))

    def to_dict(self):
        return {"name": self.name, "state": str(self.state)}


class StandardPhase(Enum):
    NONE = Phase('NONE', InstanceState.NONE)
    INIT = Phase('INIT', InstanceState.CREATED)
    TERMINAL = Phase('TERMINAL', InstanceState.ENDED)


class Lifecycle:
    """
    This class represents the lifecycle of an instance. A lifecycle consists of a chronological sequence of
    lifecycle phases. Each phase has a timestamp that indicates when the transition to that phase occurred.
    """

    def __init__(self, *phase_transitions: Tuple[Phase, datetime.datetime], termination_status=TerminationStatus.NONE):
        self._phase_transitions: OrderedDict[Phase, datetime.datetime] = OrderedDict(phase_transitions)
        self._terminal_status = termination_status

    @classmethod
    def from_dict(cls, as_dict):
        phase_transitions = ((Phase.from_dict(phase_change['phase']), util.parse_datetime(phase_change['transitioned']))
                             for phase_change in as_dict['phase_transitions'])
        return cls(*phase_transitions)

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        d = {
            "phase_transitions": [{"phase": phase.to_dict(), "transitioned": format_dt_iso(transitioned)}
                                  for phase, transitioned in self.phase_transitions],
            "phase": self.phase.to_dict(),
            "last_transition_at": format_dt_iso(self.last_transition_at),
            "created_at": format_dt_iso(self.created_at),
            "first_executing_at": format_dt_iso(self.executed_at),
            "ended_at": format_dt_iso(self.ended_at),
            "execution_time": self.execution_time.total_seconds() if self.executed_at else None,
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    @property
    def phase(self):
        return next(reversed(self._phase_transitions.keys()), StandardPhase.NONE)

    @property
    def phases(self) -> List[Phase]:
        return list(self._phase_transitions.keys())

    @property
    def phase_transitions(self) -> Iterable[Tuple[Phase, datetime.datetime]]:
        return ((phase, transitioned) for phase, transitioned in self._phase_transitions.items())

    def transitioned_at(self, phase: Phase) -> Optional[datetime.datetime]:
        return self._phase_transitions.get(phase)

    @property
    def last_transition_at(self) -> Optional[datetime.datetime]:
        return next(reversed(self._phase_transitions.values()), None)

    def state_first_at(self, state: InstanceState) -> Optional[datetime.datetime]:
        return next((ts for phase, ts in self._phase_transitions.items() if phase.state == state), None)

    def state_last_at(self, state: InstanceState) -> Optional[datetime.datetime]:
        return next((ts for phase, ts in reversed(self._phase_transitions.items()) if phase.state == state), None)

    def contains_state(self, state: InstanceState):
        return any(phase.state == state for phase in self._phase_transitions.keys())

    @property
    def is_executed(self) -> bool:
        return self.contains_state(InstanceState.EXECUTING)

    @property
    def created_at(self) -> Optional[datetime.datetime]:
        return self.state_first_at(InstanceState.CREATED)

    @property
    def executed_at(self) -> Optional[datetime.datetime]:
        return self.state_first_at(InstanceState.EXECUTING)

    @property
    def is_terminated(self):
        return bool(self._terminal_status)

    @property
    def ended_at(self) -> Optional[datetime.datetime]:
        return self.state_last_at(InstanceState.ENDED)

    @property
    def execution_time(self) -> Optional[datetime.timedelta]:
        start = self.executed_at
        if not start:
            return None

        end = self.ended_at or util.utc_now()
        return end - start

    def __copy__(self):
        copied = Lifecycle()
        copied._phase_transitions = self._phase_transitions
        return copied

    def __deepcopy__(self, memo):
        return Lifecycle(*self.phase_transitions)

    def __eq__(self, other):
        if not isinstance(other, Lifecycle):
            return NotImplemented
        return self._phase_transitions == other._phase_transitions

    def __hash__(self):
        return hash(tuple(self._phase_transitions.items()))

    def __repr__(self) -> str:
        return "{}({!r})".format(
            self.__class__.__name__, self._phase_transitions)


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

    def __init__(self, lifecycle, phases):
        self._lifecycle = lifecycle
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
                    self.next_phase(StandardPhase.TERMINAL)
                    return

                self.next_phase(phase)

            term_status = phase.execute()

        self.next_phase(StandardPhase.TERMINAL)

    def next_phase(self, phase):
        self._current_phase = phase
        # notify listeners

    def stop(self):
        with self._phase_lock:
            if self._term_status:
                return

            self._term_status = self._current_phase.stop_status
            assert self._term_status
            if self._current_phase == StandardPhase.INIT:
                # Not started yet
                self._abort = True  # Prevent phase transition...
                self.next_phase(StandardPhase.TERMINAL)

        self._current_phase.stop()


class PendingPhase(PhaseAction):

    def __init__(self, waiters):
        self._waiters = waiters

    @property
    def phase(self):
        return Phase.PENDING

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
        return Phase.QUEUED

    def execute(self):
        return self._queue_waiter.wait()

    def stop(self):
        self._queue_waiter.cancel()

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED
