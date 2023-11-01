"""
A run is an abstract concept that consists of a sequence of individual phase runs. A 'run' refers to any sequence of
phases, whether they are processes, programs, tasks, conditions, or other constructs, executed in a specific order.
Each phase has a unique name and defines its run state, which determines the nature of the phase's activity during
its run (like waiting, evaluating, executing, etc.). Phases operate in a predefined order; when one phase ends, the
subsequent phase begins. However, if a phase ends and signals premature termination by providing a status other than
'COMPLETED,' the next phase may not commence. Regardless of how the entire run finishes, the final phase must be a
termination phase, and a termination status must be provided. This module includes a class, 'Phaser,' which represents
the run concept by orchestrating the given phase steps. Each phase step represents an implementation of a phase.
"""

import datetime
import logging
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum, auto, EnumMeta
from threading import Lock, Event
from typing import Optional, List, Dict, Any, Set, TypeVar, Type, Callable

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


@dataclass(frozen=True)
class Phase:
    name: str
    state: RunState

    @classmethod
    def from_dict(cls, as_dict) -> 'Phase':
        return cls(as_dict['name'], RunState.from_str(as_dict['state']))

    def to_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "state": str(self.state)}


class StandardPhase(Enum):
    NONE = Phase('NONE', RunState.NONE)
    INIT = Phase('INIT', RunState.CREATED)
    TERMINAL = Phase('TERMINAL', RunState.ENDED)


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
    SKIPPED = {Flag.UNEXECUTED, Flag.NONSUCCESS, Flag.DISCARDED, Flag.REJECTED}
    UNSATISFIED = {Flag.UNEXECUTED, Flag.NONSUCCESS, Flag.DISCARDED, Flag.REJECTED}

    RUNNING = {Flag.EXECUTED}

    COMPLETED = {Flag.EXECUTED, Flag.SUCCESS}

    STOPPED = {Flag.EXECUTED, Flag.NONSUCCESS, Flag.INCOMPLETE, Flag.ABORTED}
    INTERRUPTED = {Flag.EXECUTED, Flag.NONSUCCESS, Flag.INCOMPLETE, Flag.ABORTED}
    FAILED = {Flag.EXECUTED, Flag.NONSUCCESS, Flag.INCOMPLETE, Flag.FAILURE}
    ERROR = {Flag.EXECUTED, Flag.NONSUCCESS, Flag.INCOMPLETE, Flag.FAILURE}

    def __new__(cls, *args, **kwds):
        value = len(cls.__members__) + 1
        obj = object.__new__(cls)
        obj._value_ = value
        return obj

    @classmethod
    def from_str(cls, value: str):
        try:
            return cls[value.upper()]
        except KeyError:
            return cls.UNKNOWN

    @classmethod
    def with_flags(cls, *flags):
        return [state for state in cls if all(flag in state.flags for flag in flags)]

    def __init__(self, flags: Set[TerminationStatusFlag]):
        self.flags = flags

    def __bool__(self):
        return self != TerminationStatus.NONE

    def has_flag(self, flag: TerminationStatusFlag):
        return flag in self.flags


@dataclass(frozen=True)
class PhaseTransition:
    phase: Phase
    transitioned: datetime.datetime


@dataclass(frozen=True)
class PhaseRun:
    phase: Phase
    started_at: datetime
    ended_at: Optional[datetime.datetime]
    execution_time: Optional[datetime.timedelta]


class Lifecycle:
    """
    This class represents the lifecycle of a run. A lifecycle consists of a chronological sequence of phase transitions.
    Each phase has a timestamp that indicates when the transition to that phase occurred.
    """

    def __init__(self, *transitions: PhaseTransition, termination_status=TerminationStatus.NONE):
        self._transitions = OrderedDict()
        self._phase_runs: List[PhaseRun] = []
        self._termination_status = termination_status
        for transition in transitions:
            self.add_transition(transition)

    def add_transition(self, new_transition: PhaseTransition, termination_status=TerminationStatus.NONE):
        """
        Adds a new phase transition to the lifecycle and updates the phase runs accordingly.
        TODO:
        1. Check phase name not already present
        2. Check termination status not already set
        """
        if new_transition.phase == StandardPhase.NONE.value or new_transition.phase == self.phase:
            return False

        if new_transition.phase.state == RunState.ENDED and not termination_status:
            raise ValueError("Termination status is mandatory for ended state phase")

        if self._transitions:
            last_phase_run = self._phase_runs[-1]
            ended_at = new_transition.transitioned
            exec_time = new_transition.transitioned - last_phase_run.started_at
            self._phase_runs[-1] = PhaseRun(last_phase_run.phase, last_phase_run.started_at, ended_at, exec_time)

        # Add the new transition.
        self._transitions[new_transition.phase] = new_transition

        # Begin a new phase run for the new transition (without an end time yet).
        self._phase_runs.append(PhaseRun(
            phase=new_transition.phase,
            started_at=new_transition.transitioned,
            ended_at=None,
            execution_time=None
        ))
        self._termination_status = termination_status

    @classmethod
    def from_dict(cls, as_dict):
        transitions = [
            PhaseTransition(Phase.from_dict(phase_change['phase']), util.parse_datetime(phase_change['transitioned']))
            for phase_change in as_dict['transitions']]
        term_status = TerminationStatus.from_str(as_dict['termination_status'])
        return cls(*transitions, termination_status=term_status)

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        d = {
            "transitions": [{"phase": pt.phase.to_dict(), "transitioned": format_dt_iso(pt.transitioned)}
                            for pt in self._transitions],
            "termination_status": str(self._termination_status),
            "phase": self.phase.to_dict(),
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
    def phase(self):
        return next(reversed(self._transitions.keys()), StandardPhase.NONE.value)

    @property
    def state(self):
        return self.phase.state

    @property
    def phase_count(self):
        return len(self._transitions)

    def get_ordinal(self, phase: Phase) -> int:
        for index, current_phase in enumerate(self._transitions.keys()):
            if current_phase == phase:
                return index + 1
        raise ValueError(f"Phase {phase} not found in lifecycle")

    @property
    def phases(self) -> List[Phase]:
        return list(self._transitions.keys())

    @property
    def phase_runs(self) -> List[PhaseRun]:
        return list(self._phase_runs)

    def phase_run(self, phase: Phase) -> Optional[PhaseRun]:
        for run in self._phase_runs:
            if run.phase == phase:
                return run

        return None

    @property
    def termination_status(self):
        return self._termination_status

    def transitioned_at(self, phase: Phase) -> Optional[datetime.datetime]:
        phase_transition = self._transitions.get(phase)
        return phase_transition.transitioned if phase_transition else None

    @property
    def last_transition_at(self) -> Optional[datetime.datetime]:
        last_transition = next(reversed(self._transitions.values()), None)
        return last_transition.transitioned if last_transition else None

    def state_first_at(self, state: RunState) -> Optional[datetime.datetime]:
        return next((pt.transitioned for pt in self._transitions.values() if pt.phase.state == state), None)

    def state_last_at(self, state: RunState) -> Optional[datetime.datetime]:
        return next((pt.transitioned for pt in reversed(self._transitions.values()) if pt.phase.state == state),
                    None)

    def contains_state(self, state: RunState):
        return any(pt.phase.state == state for pt in self._transitions.values())

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
    def is_terminated(self):
        return bool(self._termination_status)

    def run_time_in_state(self, state: RunState) -> datetime.timedelta:
        """
        Calculate the total time spent in the given state.

        Args:
            state (RunState): The state to calculate run time for.

        Returns:
            datetime.timedelta: Total time spent in the given state.
        """
        durations = [run.execution_time for run in self._phase_runs if run.phase.state == state and run.execution_time]
        return sum(durations, datetime.timedelta())

    @property
    def total_executing_time(self) -> Optional[datetime.timedelta]:
        return self.run_time_in_state(RunState.EXECUTING)

    def __copy__(self):
        copied = Lifecycle()
        copied._transitions = self._transitions.copy()
        copied._phase_runs = self._phase_runs.copy()
        return copied


class PhaseStep(ABC):

    def __init__(self, phase):
        self._phase = phase
        self._status_notification = ObservableNotification[StatusObserver]()

    @property
    def phase(self):
        return self._phase

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


class NoOpsStep(PhaseStep):

    def __init__(self, phase, stop_status):
        super().__init__(phase)
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


class InitStep(NoOpsStep):

    def __init__(self):
        super().__init__(StandardPhase.INIT.value, TerminationStatus.STOPPED)


class TerminalStep(NoOpsStep):

    def __init__(self):
        super().__init__(StandardPhase.TERMINAL.value, TerminationStatus.NONE)


class WaitWrapperStep(PhaseStep):

    def __init__(self, wrapped_step):
        super().__init__(wrapped_step.phase)
        self.wrapped_step = wrapped_step
        self._run_event = Event()

    @property
    def stop_status(self):
        return self.wrapped_step.stop_status

    def wait(self, timeout):
        self._run_event.wait(timeout)

    def run(self):
        self._run_event.set()
        self.wrapped_step.run()

    def stop(self):
        self.wrapped_step.stop()


def unique_steps_to_dict(steps):
    name_to_step = {}
    for step in steps:
        if step.phase.name in name_to_step:
            raise ValueError(f"Duplicate phase found: {step.phase.name}")
        name_to_step[step.phase.name] = step
    return name_to_step


class Phaser:

    def __init__(self, steps, lifecycle=None, *, timestamp_generator=util.utc_now):
        self._name_to_step = unique_steps_to_dict(steps)
        self._timestamp_generator = timestamp_generator
        self.transition_hook: Optional[Callable[[Phase, Phase, int], None]] = None
        self.status_hook: Optional[Callable[[str, bool], None]] = None

        self._phase_lock = Lock()
        # Guarded by the phase lock:
        self._lifecycle = lifecycle or Lifecycle()
        self._current_step = None
        self._abort = False
        self._term_status = TerminationStatus.NONE
        # ----------------------- #

        self._run_failure: Optional[Fault] = None
        self._run_error: Optional[Fault] = None

    T = TypeVar('T')

    def get_typed_phase_step(self, step_type: Type[T], phase_name: str) -> Optional[T]:
        step = self._name_to_step.get(phase_name)
        if step is None:
            return None

        if not isinstance(step, step_type):
            raise TypeError(f"Expected step of type {step_type}, but got {type(step)} for phase '{phase_name}'")

        return step

    @property
    def lifecycle(self):
        return self._lifecycle

    @property
    def run_error(self):
        return self._run_error

    @property
    def termination_status(self):
        return self._term_status

    @property
    def run_failure(self):
        return self._run_failure

    def prime(self):
        with self._phase_lock:
            if self._current_step:
                raise InvalidStateError("Primed already")
            self._next_step(InitStep())

    def run(self):
        if not self._current_step:
            raise InvalidStateError('Prime not executed before run')

        term_status = None
        exc = None
        for step in self._name_to_step.values():
            with self._phase_lock:
                if self._abort:
                    return
                if term_status and not self._term_status:
                    self._term_status = term_status
                if exc:
                    assert self._term_status
                    self._next_step(TerminalStep())
                    raise exc
                if self._term_status:
                    self._next_step(TerminalStep())
                    return

                self._next_step(step)

            term_status, exc = self._run_handle_errors(step)

        self._term_status = self._term_status or TerminationStatus.COMPLETED
        self._next_step(TerminalStep())

    def _run_handle_errors(self, step):
        try:
            return step.run(), None
        except FailedRun as e:
            self._run_failure = e.fault
            return TerminationStatus.FAILED, None
        except Exception as e:
            self._run_error = Fault(e.__class__.__name__, str(e))
            return TerminationStatus.ERROR, None
        except KeyboardInterrupt as e:
            log.warning('keyboard_interruption')
            # Assuming child processes received SIGINT, TODO different state on other platforms?
            return TerminationStatus.INTERRUPTED, e
        except SystemExit as e:
            # Consider UNKNOWN (or new state DETACHED?) if there is possibility the execution is not completed
            return (TerminationStatus.COMPLETED if e.code == 0 else TerminationStatus.FAILED), e

    def _next_step(self, step):
        """
        Impl note: The execution must be guarded by the phase lock (except terminal step)
        """
        assert self._current_step != step

        prev_phase = self._lifecycle.phase
        self._current_step = step
        self._current_step.status_hook = self.status_hook  # Should be the hook removed from the prev step?
        self._lifecycle.add_transition(PhaseTransition(step.phase, self._timestamp_generator()), self._term_status)
        ordinal = self._lifecycle.phase_count
        if self.transition_hook:
            self.transition_hook(prev_phase, self._lifecycle.phase, ordinal)

    def stop(self):
        with self._phase_lock:
            if self._term_status:
                return

            self._term_status = self._current_step.stop_status if self._current_step else TerminationStatus.STOPPED
            assert self._term_status
            if not self._current_step or (self._current_step.phase == StandardPhase.INIT.value):
                # Not started yet
                self._abort = True  # Prevent phase transition...
                self._next_step(TerminalStep())

        self._current_step.stop()


@dataclass
class Fault:
    fault_type: str
    reason: str


class FailedRun(Exception):
    """
    This exception is used to provide additional information about a run failure.
    """

    def __init__(self, fault_type: str, reason: str):
        super().__init__(f"{fault_type}: {reason}")
        self.fault = Fault(fault_type, reason)
