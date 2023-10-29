"""
A run is an abstract concept consisting of a sequence of individual phase runs where each phase has its unique name
and defines its run state. A terminated run has an information describing the cause of the termination in a form
of one of the predefined termination statuses. This module has a class Phaser which implements the run concept
by orchestrating provided phase implementations. The phaser expect an implementation of a phase to be an instance
of the PhaseStep interface.
"""

import datetime
import logging
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum, auto, EnumMeta
from threading import Lock
from typing import Optional, List, Dict, Any, Set

from tarotools.taro import util
from tarotools.taro.err import InvalidStateError
from tarotools.taro.util import format_dt_iso, is_empty

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


@dataclass
class PhaseTransition:
    phase: Phase
    transitioned: datetime


class Lifecycle:
    """
    This class represents the lifecycle of an instance. A lifecycle consists of a chronological sequence of
    lifecycle phases. Each phase has a timestamp that indicates when the transition to that phase occurred.
    """

    def __init__(self, *transitions: PhaseTransition, termination_status=TerminationStatus.NONE):
        self._transitions = OrderedDict((pt.phase, pt) for pt in transitions)
        self._terminal_status = termination_status

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
                            for pt in self.phase_transitions],
            "termination_status": str(self._terminal_status),
            "phase": self.phase.to_dict(),
            "last_transition_at": format_dt_iso(self.last_transition_at),
            "created_at": format_dt_iso(self.created_at),
            "executed_at": format_dt_iso(self.executed_at),
            "ended_at": format_dt_iso(self.ended_at),
            "execution_time": self.execution_time.total_seconds() if self.ended_at else None,
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    @property
    def phase(self):
        return next(reversed(self._transitions.keys()), StandardPhase.NONE)

    @property
    def phase_count(self):
        return len(self._transitions)

    def get_ordinal(self, phase: Phase) -> int:
        for index, current_phase in enumerate(self._transitions.keys()):
            if current_phase == phase:
                return index
        raise ValueError(f"Phase {phase} not found in lifecycle")

    @property
    def phases(self) -> List[Phase]:
        return list(self._transitions.keys())

    @property
    def phase_transitions(self) -> List[PhaseTransition]:
        return list(self._transitions.values())

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
    def is_executed(self) -> bool:
        return self.contains_state(RunState.EXECUTING)

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
        return bool(self._terminal_status)

    @property
    def execution_time(self) -> Optional[datetime.timedelta]:
        """
        Change to total executing time.
        """
        start = self.executed_at
        if not start:
            return None

        end = self.ended_at or util.utc_now()
        return end - start

    def __copy__(self):
        copied = Lifecycle()
        copied._transitions = self._transitions.copy()
        return copied

    def __deepcopy__(self, memo):
        copied = Lifecycle(*self.phase_transitions)
        return copied


class MutableLifecycle(Lifecycle):
    """
    Mutable version of `InstanceLifecycle`
    """

    def __init__(self,
                 *transitions: PhaseTransition,
                 termination_status=TerminationStatus.NONE,
                 timestamp_generator=util.utc_now):
        super().__init__(*transitions, termination_status=termination_status)
        self._ts_generator = timestamp_generator

    def new_phase(self, new_phase: Phase, terminal_status=TerminationStatus.NONE) -> bool:
        if not new_phase or new_phase == StandardPhase.NONE or self.phase == new_phase:
            return False

        self._transitions[new_phase] = PhaseTransition(new_phase, self._ts_generator())
        self._terminal_status = terminal_status
        return True


class PhaseStep(ABC):

    @property
    @abstractmethod
    def phase(self):
        pass

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @property
    @abstractmethod
    def stop_status(self):
        pass


class InitStep(PhaseStep):

    @property
    def phase(self):
        return StandardPhase.INIT

    def run(self):
        pass

    def stop(self):
        pass

    @property
    def stop_status(self):
        return TerminationStatus.STOPPED


class TerminalStep(PhaseStep):

    @property
    def phase(self):
        return StandardPhase.TERMINAL

    def run(self):
        pass

    def stop(self):
        pass

    @property
    def stop_status(self):
        return TerminationStatus.NONE


class Phaser:

    def __init__(self, lifecycle, steps):
        self.transition_hook = None
        self._steps = steps

        self._phase_lock = Lock()
        # Guarded by the phase lock:
        self._lifecycle = lifecycle
        self._current_step = None
        self._abort = False
        self._run_error = None
        self._term_status = TerminationStatus.NONE

    @property
    def lifecycle(self):
        return self._lifecycle

    @property
    def run_error(self):
        return self._run_error

    @property
    def termination_status(self):
        return self._term_status

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
        for step in self._steps:
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

        self._next_step(TerminalStep())

    def _run_handle_errors(self, step):
        try:
            return step.run(), None
        except ExecutionError as e:
            self._run_error = e
            # TODO Print exception
            return self._run_error.termination_status or TerminationStatus.FAILED, None
        except Exception as e:
            self._run_error = ExecutionError.from_unexpected_exception(e)
            return TerminationStatus.ERROR, e
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
        self._lifecycle.new_phase(step.phase, self._term_status)
        ordinal = self._lifecycle.phase_count
        if self.transition_hook:
            self.transition_hook(prev_phase, self._lifecycle.phase, ordinal)

    def stop(self):
        with self._phase_lock:
            if self._term_status:
                return

            self._term_status = self._current_step.stop_status
            assert self._term_status
            if self._current_step.phase == StandardPhase.INIT:
                # Not started yet
                self._abort = True  # Prevent phase transition...
                self._next_step(TerminalStep())

        self._current_step.stop()


class ExecutionError(Exception):
    """
    This exception is used to provide additional information about an error condition that occurred during execution.
    TODO:
    1. Rename RunError
    """

    @classmethod
    def from_unexpected_exception(cls, e: Exception):
        return cls("Unexpected error: " + str(e), TerminationStatus.ERROR, unexpected_error=e)

    @classmethod
    def from_dict(cls, as_dict):
        return cls(as_dict['message'], TerminationStatus[as_dict['state']])  # TODO Add kwargs

    def __init__(self, message: str, exec_state: TerminationStatus, unexpected_error: Exception = None, **kwargs):
        if not exec_state.has_flag(TerminationStatusFlag.FAILURE):
            raise ValueError('exec_state must be flagged as failure', exec_state)
        super().__init__(message)
        self.message = message
        self.termination_status = exec_state
        self.unexpected_error = unexpected_error
        self.params = kwargs

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        # TODO Add kwargs
        d = {
            "message": self.message,
            "state": self.termination_status.name,
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    def __hash__(self):
        """
        Used for comparing in tests
        """
        return hash((self.message, self.termination_status, self.unexpected_error, self.params))

    def __eq__(self, other):
        """
        Used for comparing in tests
        """
        if not isinstance(other, ExecutionError):
            return NotImplemented
        return (self.message, self.termination_status, self.unexpected_error, self.params) == (
            other.message, other.termination_status, other.unexpected_error, self.params)
