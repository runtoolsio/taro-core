"""
A run is an abstract concept that consists of a sequence of individual phase runs. A 'run' refers to any sequence of
phases, whether they are processes, programs, tasks, conditions, or other constructs, executed in a specific order.
Each phase has a unique name and defines its run state, which determines the nature of the phase's activity during
its run (like waiting, evaluating, executing, etc.). Phases operate in a predefined order; when one phase ends, the
subsequent phase begins. However, if a phase ends and signals premature termination by providing a termination status,
the next phase may not commence. Regardless of how the entire run finishes, the final phase must be a terminal phase,
and a termination status must be provided. This module includes a class, 'Phaser,' which implements the run concept
by orchestrating the given phase phases.
"""

import datetime
import logging
from abc import ABC, abstractmethod
from collections import OrderedDict
from copy import copy
from dataclasses import dataclass
from enum import Enum, auto, EnumMeta
from threading import Event, RLock
from typing import Optional, List, Dict, Any, Set, TypeVar, Type, Callable, Tuple, Iterable

from tarotools.taro import util, status
from tarotools.taro.err import InvalidStateError
from tarotools.taro.status import StatusObserver
from tarotools.taro.util import format_dt_iso, is_empty
from tarotools.taro.util.observer import ObservableNotification

log = logging.getLogger(__name__)


class RunStateMeta(EnumMeta):
    def __getitem__(self, item):
        if isinstance(item, str):
            try:
                return super().__getitem__(item.upper())
            except KeyError:
                return RunState.UNKNOWN
        return super().__getitem__(item)


class RunState(Enum):
    NONE = auto()
    UNKNOWN = auto()
    CREATED = auto()
    PENDING = auto()
    WAITING = auto()
    EVALUATING = auto()
    IN_QUEUE = auto()
    EXECUTING = auto()
    ENDED = auto()

    def __call__(self, lifecycle):
        if self == RunState.ENDED:
            return lifecycle.state_last_at(self)
        else:
            return lifecycle.state_first_at(self)

    @classmethod
    def from_str(cls, value: str):
        try:
            return cls[value.upper()]
        except KeyError:
            return cls.UNKNOWN


class TerminationStatusFlag(Enum):
    BEFORE_EXECUTION = auto()  # Not yet executed
    UNEXECUTED = auto()  # Not yet executed or reached terminal phase without execution.
    WAITING = auto()  # Waiting for a condition before execution.
    DISCARDED = auto()  # Discarded automatically or by the user before execution.
    REJECTED = auto()  # Automatically rejected before execution.
    EXECUTED = auto()  # Reached the executing state.
    SUCCESS = auto()  # Completed successfully.
    NONSUCCESS = auto()  # Not completed successfully either before or after execution.
    INCOMPLETE = auto()  # Not completed successfully after execution.
    FAILURE = auto()  # Failed after or before execution.
    ABORTED = auto()  # Interrupted by the user after or before execution.


class TerminationStatusMeta(EnumMeta):

    def __getitem__(self, name):
        if not name:
            return TerminationStatus.NONE
        try:
            return super().__getitem__(name.upper())
        except KeyError:
            return TerminationStatus.UNKNOWN


Flag = TerminationStatusFlag


class TerminationStatus(Enum, metaclass=TerminationStatusMeta):
    NONE = {}
    UNKNOWN = {}

    CREATED = {Flag.BEFORE_EXECUTION, Flag.UNEXECUTED}

    PENDING = {Flag.BEFORE_EXECUTION, Flag.UNEXECUTED, Flag.WAITING}  # Until released
    QUEUED = {Flag.BEFORE_EXECUTION, Flag.UNEXECUTED, Flag.WAITING}  # Wait for another job

    CANCELLED = {Flag.UNEXECUTED, Flag.NONSUCCESS, Flag.DISCARDED, Flag.ABORTED}
    TIMEOUT = {Flag.UNEXECUTED, Flag.NONSUCCESS, Flag.DISCARDED, Flag.REJECTED}
    INVALID_OVERLAP = {Flag.UNEXECUTED, Flag.NONSUCCESS, Flag.DISCARDED, Flag.REJECTED}
    UNSATISFIED = {Flag.UNEXECUTED, Flag.NONSUCCESS, Flag.DISCARDED, Flag.REJECTED}

    RUNNING = {Flag.EXECUTED}

    COMPLETED = {Flag.EXECUTED, Flag.SUCCESS}

    STOPPED = {Flag.EXECUTED, Flag.NONSUCCESS, Flag.INCOMPLETE, Flag.ABORTED}
    INTERRUPTED = {Flag.EXECUTED, Flag.NONSUCCESS, Flag.INCOMPLETE, Flag.ABORTED}
    FAILED = {Flag.EXECUTED, Flag.NONSUCCESS, Flag.INCOMPLETE, Flag.FAILURE}
    ERROR = {Flag.EXECUTED, Flag.NONSUCCESS, Flag.INCOMPLETE, Flag.FAILURE}

    def __new__(cls, *_, **__):
        value = len(cls.__members__) + 1
        obj = object.__new__(cls)
        obj._value_ = value
        return obj

    @classmethod
    def with_flags(cls, *flags):
        return [state for state in cls if all(flag in state.flags for flag in flags)]

    def __init__(self, flags: Set[TerminationStatusFlag]):
        self.flags = flags

    def __bool__(self):
        return self != TerminationStatus.NONE

    def has_flag(self, flag: TerminationStatusFlag):
        return flag in self.flags


class StandardPhaseNames:
    INIT = 'INIT'
    TERMINAL = 'TERMINAL'


@dataclass
class PhaseRun:
    phase_name: str
    run_state: RunState
    started_at: datetime.datetime
    ended_at: Optional[datetime.datetime] = None

    @classmethod
    def deserialize(cls, d):
        return cls(
            d['phase_name'],
            RunState[d['run_state']],
            util.parse_datetime(d['started_at']),
            util.parse_datetime(d['ended_at'])
        )

    def serialize(self):
        return {
            'phase_name': self.phase_name,
            'run_state': self.run_state.name,
            'started_at': format_dt_iso(self.started_at),
            'ended_at': format_dt_iso(self.ended_at)
        }

    @property
    def run_time(self):
        return self.ended_at - self.started_at

    def __copy__(self):
        return PhaseRun(self.phase_name, self.run_state, self.started_at, self.ended_at)


class Lifecycle:
    """
    This class represents the lifecycle of a run. A lifecycle consists of a chronological sequence of phase transitions.
    Each phase has a timestamp that indicates when the transition to that phase occurred.
    """

    def __init__(self, *phase_runs: PhaseRun):
        self._phase_runs: OrderedDict[str, PhaseRun] = OrderedDict()
        self._current_run: Optional[PhaseRun] = None
        self._previous_run: Optional[PhaseRun] = None
        for run in phase_runs:
            self.add_phase_run(run)

    def add_phase_run(self, phase_run: PhaseRun):
        """
        Adds a new phase run to the lifecycle.
        """
        if phase_run.phase_name in self._phase_runs:
            raise ValueError(f"Phase {phase_run.phase_name} already in this lifecycle: {self.phases}")

        if self.current_run:
            self._previous_run = self._current_run
            self._previous_run.ended_at = phase_run.started_at

        self._current_run = phase_run
        self._phase_runs[phase_run.phase_name] = phase_run

    @classmethod
    def deserialize(cls, as_dict):
        phase_runs = [PhaseRun.deserialize(run) for run in as_dict['phase_runs']]
        return cls(*phase_runs)

    def serialize(self) -> Dict[str, Any]:
        return {"phase_runs": [run.serialize() for run in self._phase_runs.values()]}

    def dto(self, include_empty=True) -> Dict[str, Any]:
        d = {
            "phase_runs": [run.serialize() for run in self._phase_runs.values()],
            "current_run": self.current_run.serialize(),
            "previous_run": self.previous_run.serialize(),
            "last_transition_at": format_dt_iso(self.last_transition_at),
            "created_at": format_dt_iso(self.created_at),
            "executed_at": format_dt_iso(self.executed_at),
            "ended_at": format_dt_iso(self.ended_at),
            "execution_time": self.total_executing_time.total_seconds() if self.ended_at else None,
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    @property
    def current_run(self) -> Optional[PhaseRun]:
        return self._current_run

    @property
    def current_phase(self) -> Optional[str]:
        return self._current_run.phase_name if self._current_run else None

    @property
    def previous_run(self) -> Optional[PhaseRun]:
        return self._previous_run

    @property
    def previous_phase(self) -> Optional[str]:
        return self._previous_run.phase_name if self._previous_run else None

    @property
    def run_state(self):
        if not self._current_run:
            return RunState.NONE

        return self._current_run.run_state

    @property
    def phase_count(self):
        return len(self._phase_runs)

    def get_ordinal(self, phase_name: str) -> int:
        for index, current_phase in enumerate(self._phase_runs.keys()):
            if current_phase == phase_name:
                return index + 1
        raise ValueError(f"Phase {phase_name} not found in lifecycle")

    @property
    def phases(self) -> List[str]:
        return list(self._phase_runs.keys())

    @property
    def phase_runs(self) -> List[PhaseRun]:
        return list(self._phase_runs.values())

    def phase_run(self, phase_name: str) -> Optional[PhaseRun]:
        return self._phase_runs.get(phase_name)

    def runs_between(self, phase_from, phase_to) -> List[PhaseRun]:
        runs = []
        for run in self._phase_runs.values():
            if run.phase_name == phase_to:
                if not runs:
                    if phase_from == phase_to:
                        return [run]
                    else:
                        return []
                runs.append(run)
                return runs
            elif run.phase_name == phase_from or runs:
                runs.append(run)

        return []

    def phases_between(self, phase_from, phase_to):
        return [run.phase_name for run in self.runs_between(phase_from, phase_to)]

    def phase_started_at(self, phase_name: str) -> Optional[datetime.datetime]:
        phase_run = self._phase_runs.get(phase_name)
        return phase_run.started_at if phase_run else None

    @property
    def last_transition_at(self) -> Optional[datetime.datetime]:
        if not self._current_run:
            return None

        return self._current_run.started_at

    def state_first_at(self, state: RunState) -> Optional[datetime.datetime]:
        return next((run.started_at for run in self._phase_runs.values() if run.run_state == state), None)

    def state_last_at(self, state: RunState) -> Optional[datetime.datetime]:
        return next((run.started_at for run in reversed(self._phase_runs.values()) if run.run_state == state), None)

    def contains_state(self, state: RunState):
        return any(run.run_state == state for run in self._phase_runs.values())

    @property
    def created_at(self) -> Optional[datetime.datetime]:
        return self.state_first_at(RunState.CREATED)

    @property
    def executed_at(self) -> Optional[datetime.datetime]:
        return self.state_first_at(RunState.EXECUTING)

    @property
    def ended_at(self) -> Optional[datetime.datetime]:
        return self.state_last_at(RunState.ENDED)

    @property
    def is_ended(self):
        return self.contains_state(RunState.ENDED)

    def run_time_in_state(self, state: RunState) -> datetime.timedelta:
        """
        Calculate the total time spent in the given state.

        Args:
            state (RunState): The state to calculate run time for.

        Returns:
            datetime.timedelta: Total time spent in the given state.
        """
        durations = [run.run_time for run in self._phase_runs.values() if run.run_state == state and run.run_time]
        return sum(durations, datetime.timedelta())

    @property
    def total_executing_time(self) -> Optional[datetime.timedelta]:
        return self.run_time_in_state(RunState.EXECUTING)

    def __copy__(self):
        copied = Lifecycle()
        copied._phase_runs = OrderedDict((name, copy(run)) for name, run in self._phase_runs.items())
        copied._current_run = copy(self._current_run)
        copied._previous_run = copy(self._previous_run)
        return copied


@dataclass
class PhaseMetadata:
    phase_name: str
    run_state: RunState
    parameters: Dict[str, str]

    @classmethod
    def deserialize(cls, as_dict) -> 'PhaseMetadata':
        return cls(as_dict["phase_name"], RunState[as_dict["run_state"]], as_dict["parameters"] or {})

    def serialize(self):
        return {"phase_name": self.phase_name, "run_state": self.run_state.name, "parameters": self.parameters}


class Phase(ABC):

    def __init__(self, phase_name: str, run_state: RunState, parameters: Optional[Dict[str, str]] = None):
        self._metadata = PhaseMetadata(phase_name, run_state, parameters or {})
        self._status_notification = ObservableNotification[StatusObserver]()

    @property
    def name(self):
        return self._metadata.phase_name

    @property
    def metadata(self):
        return self._metadata

    @property
    @abstractmethod
    def stop_status(self):
        pass

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    def add_status_observer(self, observer, priority=status.DEFAULT_OBSERVER_PRIORITY):
        self._status_notification.add_observer(observer, priority)

    def remove_status_observer(self, observer):
        self._status_notification.remove_observer(observer)


class NoOpsPhase(Phase):

    def __init__(self, phase_name, run_state, stop_status):
        super().__init__(phase_name, run_state)
        self._stop_status = stop_status

    @property
    def stop_status(self):
        return self._stop_status

    def run(self):
        """No activity on run"""
        pass

    def stop(self):
        """Nothing to stop"""
        pass


class InitPhase(NoOpsPhase):

    def __init__(self):
        super().__init__(StandardPhaseNames.INIT, RunState.CREATED, TerminationStatus.STOPPED)


class TerminalPhase(NoOpsPhase):

    def __init__(self):
        super().__init__(StandardPhaseNames.TERMINAL, RunState.ENDED, TerminationStatus.NONE)


class WaitWrapperPhase(Phase):

    def __init__(self, wrapped_phase):
        super().__init__(wrapped_phase.name, wrapped_phase.metadata.run_state, wrapped_phase.metadata.parameters)
        self.wrapped_phase = wrapped_phase
        self._run_event = Event()

    @property
    def stop_status(self):
        return self.wrapped_phase.stop_status

    def wait(self, timeout):
        self._run_event.wait(timeout)

    def run(self):
        self._run_event.set()
        self.wrapped_phase.run()

    def stop(self):
        self.wrapped_phase.stop()


@dataclass
class Fault:
    fault_type: str
    reason: str

    def serialize(self):
        return {"fault_type": self.fault_type, "reason": self.reason}

    @classmethod
    def deserialize(cls, as_dict):
        return cls(as_dict["fault_type"], as_dict["reason"])


@dataclass
class RunFailure(Fault):
    pass


@dataclass
class RunError(Fault):
    pass


class FailedRun(Exception):
    """
    This exception is used to provide additional information about a run failure.
    """

    def __init__(self, fault_type: str, reason: str):
        super().__init__(f"{fault_type}: {reason}")
        self.fault = RunFailure(fault_type, reason)


@dataclass(frozen=True)
class TerminationInfo:
    termination_status: TerminationStatus
    terminated_at: datetime.datetime
    failure: Optional[Fault] = None
    error: Optional[Fault] = None


@dataclass(frozen=True)
class RunSnapshot:
    phases: Tuple[PhaseMetadata]
    lifecycle: Lifecycle
    termination: TerminationInfo


def unique_phases_to_dict(phases) -> Dict[str, Phase]:
    name_to_phase = {}
    for phase in phases:
        if phase.name in name_to_phase:
            raise ValueError(f"Duplicate phase found: {phase.name}")
        name_to_phase[phase.name] = phase
    return name_to_phase


class Phaser:

    def __init__(self, phases: Iterable[Phase], lifecycle=None, *, timestamp_generator=util.utc_now):
        self._name_to_phase: Dict[str, Phase] = unique_phases_to_dict(phases)
        self._phase_meta: Tuple[PhaseMetadata] = tuple(phase.metadata for phase in phases)
        self._timestamp_generator = timestamp_generator
        self.transition_hook: Optional[Callable[[PhaseRun, PhaseRun, int], None]] = None

        self._transition_lock = RLock()
        # Guarded by the transition/state lock:
        self._lifecycle = lifecycle or Lifecycle()
        self._current_phase = None
        self._abort = False
        self._termination: Optional[TerminationInfo] = None
        # ----------------------- #

    P = TypeVar('P')

    def get_typed_phase(self, phase_type: Type[P], phase_name: str) -> Optional[P]:
        phase = self._name_to_phase.get(phase_name)
        if phase is None:
            return None

        if not isinstance(phase, phase_type):
            raise TypeError(f"Expected phase of type {phase_type}, but got {type(phase)} for phase '{phase_name}'")

        return phase

    @property
    def phases(self) -> List[Phase]:
        return list(self._name_to_phase.values())

    def create_run_snapshot(self) -> RunSnapshot:
        with self._transition_lock:
            return RunSnapshot(self._phase_meta, copy(self._lifecycle), self._termination)

    def prime(self):
        with self._transition_lock:
            if self._current_phase:
                raise InvalidStateError("Primed already")
            self._next_phase(InitPhase())

    def run(self):
        if not self._current_phase:
            raise InvalidStateError('Prime not executed before run')

        term_info = None
        exc = None
        for phase in self._name_to_phase.values():
            with self._transition_lock:
                if self._abort:
                    return
                if term_info and not self._termination:  # Set only when not set by stop already
                    self._termination = term_info
                if isinstance(exc, BaseException):
                    assert self._termination
                    self._next_phase(TerminalPhase())
                    raise exc
                if self._termination:
                    self._next_phase(TerminalPhase())
                    return

                self._next_phase(phase)

            term_info, exc = self._run_handle_errors(phase)

        self._termination = (
                self._termination or TerminationInfo(TerminationStatus.COMPLETED, self._timestamp_generator()))
        self._next_phase(TerminalPhase())

    def _run_handle_errors(self, phase) -> Tuple[Optional[TerminationInfo], Optional[BaseException]]:
        term_ts = self._timestamp_generator()

        try:
            return phase.run(), None
        except FailedRun as e:
            return TerminationInfo(TerminationStatus.FAILED, term_ts, e.fault), None
        except Exception as e:
            run_error = RunError(e.__class__.__name__, str(e))
            return TerminationInfo(TerminationStatus.ERROR, term_ts, run_error), None
        except KeyboardInterrupt as e:
            log.warning('keyboard_interruption')
            # Assuming child processes received SIGINT, TODO different state on other platforms?
            return TerminationInfo(TerminationStatus.INTERRUPTED, term_ts), e
        except SystemExit as e:
            # Consider UNKNOWN (or new state DETACHED?) if there is possibility the execution is not completed
            term_status = TerminationStatus.COMPLETED if e.code == 0 else TerminationStatus.FAILED
            return TerminationInfo(term_status, term_ts), e

    def _next_phase(self, phase):
        """
        Impl note: The execution must be guarded by the phase lock (except terminal phase)
        """
        assert self._current_phase != phase

        self._current_phase = phase
        self._lifecycle.add_phase_run(PhaseRun(phase.name, phase.metadata.run_state, self._timestamp_generator()))
        if self.transition_hook:
            self.execute_transition_hook_safely(self.transition_hook)

    def execute_transition_hook_safely(self, transition_hook: Optional[Callable[[PhaseRun, PhaseRun, int], None]]):
        with self._transition_lock:
            lc = copy(self._lifecycle)
            transition_hook(lc.previous_run, lc.current_run, lc.phase_count)

    def stop(self):
        with self._transition_lock:
            if self._termination:
                return

            stop_status = self._current_phase.stop_status if self._current_phase else TerminationStatus.STOPPED
            self._termination = TerminationInfo(stop_status, self._timestamp_generator())
            assert self._termination.termination_status
            if not self._current_phase or (self._current_phase.name == StandardPhaseNames.INIT):
                # Not started yet
                self._abort = True  # Prevent phase transition...
                self._next_phase(TerminalPhase())

        self._current_phase.stop()
