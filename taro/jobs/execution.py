"""
Execution framework defines an abstraction for an execution of a task.
It consists of:
 1. Possible states of execution
 2. A structure for conveying of error conditions
 3. An interface for implementing various types of executions
"""

import abc
import datetime
from collections import OrderedDict
from enum import Enum, auto, EnumMeta
from typing import Tuple, List, Iterable, Set, Optional, Dict, Any

from taro import util
from taro.util import utc_now, format_dt_iso, is_empty


class ExecutionStateGroup(Enum):
    BEFORE_EXECUTION = auto()
    WAITING = auto()
    EXECUTING = auto()
    TERMINAL = auto()
    SUCCESS = auto()
    NOT_COMPLETED = auto()
    NOT_EXECUTED = auto()
    FAILURE = auto()


class ExecutionStateMeta(EnumMeta):
    def __getitem__(self, name):
        try:
            return super().__getitem__(name)
        except KeyError:
            return ExecutionState.UNKNOWN


class ExecutionState(Enum, metaclass=ExecutionStateMeta):
    NONE = {}
    UNKNOWN = {}
    CREATED = {ExecutionStateGroup.BEFORE_EXECUTION}

    PENDING = {ExecutionStateGroup.WAITING, ExecutionStateGroup.BEFORE_EXECUTION}  # Until released
    WAITING = {ExecutionStateGroup.WAITING, ExecutionStateGroup.BEFORE_EXECUTION}  # Wait for another job
    # ON_HOLD or same as pending?

    TRIGGERED = {ExecutionStateGroup.EXECUTING}  # Start request sent, start confirmation not (yet) received
    STARTED = {ExecutionStateGroup.EXECUTING}
    RUNNING = {ExecutionStateGroup.EXECUTING}

    COMPLETED = {ExecutionStateGroup.TERMINAL, ExecutionStateGroup.SUCCESS}

    STOPPED = {ExecutionStateGroup.TERMINAL, ExecutionStateGroup.NOT_COMPLETED}
    INTERRUPTED = {ExecutionStateGroup.TERMINAL, ExecutionStateGroup.NOT_COMPLETED}

    DISABLED = {ExecutionStateGroup.TERMINAL, ExecutionStateGroup.NOT_EXECUTED}
    CANCELLED = {ExecutionStateGroup.TERMINAL, ExecutionStateGroup.NOT_EXECUTED}
    SKIPPED = {ExecutionStateGroup.TERMINAL, ExecutionStateGroup.NOT_EXECUTED}
    DEPENDENCY_NOT_RUNNING = {ExecutionStateGroup.TERMINAL, ExecutionStateGroup.NOT_EXECUTED}
    SUSPENDED = {ExecutionStateGroup.TERMINAL, ExecutionStateGroup.NOT_EXECUTED}  # Temporarily disabled

    START_FAILED = {ExecutionStateGroup.TERMINAL, ExecutionStateGroup.FAILURE}
    FAILED = {ExecutionStateGroup.TERMINAL, ExecutionStateGroup.FAILURE}
    ERROR = {ExecutionStateGroup.TERMINAL, ExecutionStateGroup.FAILURE}

    def __new__(cls, *args, **kwds):
        value = len(cls.__members__) + 1
        obj = object.__new__(cls)
        obj._value_ = value
        return obj

    @classmethod
    def get_states_by_group(cls, exec_group):
        return [state for state in cls if exec_group in state.groups]

    def __init__(self, groups: Set[ExecutionStateGroup]):
        self.groups = groups

    def is_before_execution(self):
        return ExecutionStateGroup.BEFORE_EXECUTION in self.groups

    def is_waiting(self):
        return ExecutionStateGroup.WAITING in self.groups

    def is_executing(self):
        return ExecutionStateGroup.EXECUTING in self.groups

    def is_incomplete(self):
        return ExecutionStateGroup.NOT_COMPLETED in self.groups

    def is_unexecuted(self):
        return ExecutionStateGroup.NOT_EXECUTED in self.groups

    def is_terminal(self) -> bool:
        return ExecutionStateGroup.TERMINAL in self.groups

    def is_failure(self) -> bool:
        return ExecutionStateGroup.FAILURE in self.groups  # TODO convert to `failure` property?


class UnexpectedStateError(Exception):
    """
    Raised when a processing logic cannot recognize an execution state or
    when the state is not valid in a given context.
    """


class ExecutionError(Exception):

    @classmethod
    def from_unexpected_error(cls, e: Exception):
        return cls("Unexpected error", ExecutionState.ERROR, unexpected_error=e)

    @classmethod
    def from_dict(cls, as_dict):
        return cls(as_dict['message'], ExecutionState[as_dict['state']])

    def __init__(self, message: str, exec_state: ExecutionState, unexpected_error: Exception = None, **kwargs):
        if not exec_state.is_failure():
            raise ValueError('exec_state must be a failure', exec_state)
        super().__init__(message)
        self.message = message
        self.exec_state = exec_state
        self.unexpected_error = unexpected_error
        self.params = kwargs

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        d = {
            "message": self.message,
            "state": self.exec_state.name,
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    def __hash__(self):
        return hash((self.message, self.exec_state, self.unexpected_error))

    def __eq__(self, other):
        if not isinstance(other, ExecutionError):
            return NotImplemented
        return (self.message, self.exec_state, self.unexpected_error) == (
            other.message, other.exec_state, other.unexpected_error)


class Execution(abc.ABC):

    @property
    @abc.abstractmethod
    def is_async(self) -> bool:
        """
        TODO Delete
        SYNCHRONOUS TASK
            - finishes after the call of the execute() method
            - execution state is changed to RUNNING before the call of the execute() method

        ASYNCHRONOUS TASK
            - need not to finish after the call of the execute() method
            - execution state is changed to TRIGGER before the call of the execute() method

        :return: whether this execution is asynchronous
        """

    @abc.abstractmethod
    def execute(self) -> ExecutionState:
        """
        Executes a task

        :return: state after the execution of this method
        :raises ExecutionError: when execution failed
        :raises Exception: on unexpected failure
        """

    @property
    @abc.abstractmethod
    def tracking(self):
        """
        :return: an object containing tracking information about the progress of the execution
        """

    @property
    @abc.abstractmethod
    def status(self):
        """
        If progress monitoring is not supported then this method will always return None otherwise:
         - if executing -> current progress
         - when finished -> result

        :return: progress/result or None
        """

    @property
    @abc.abstractmethod
    def parameters(self):
        """Sequence of tuples representing arbitrary immutable execution parameters"""

    @abc.abstractmethod
    def stop(self):
        """
        If not yet executed: Do not execute
        If already executing: Stop running execution
        If execution finished: Ignore
        """

    @abc.abstractmethod
    def interrupted(self):
        """
        Keyboard interruption signal received
        Up to the implementation how to handle it
        """


class OutputExecution(Execution):

    @abc.abstractmethod
    def add_output_observer(self, observer):
        """
        Register output observer

        :param observer observer to register
        """

    @abc.abstractmethod
    def remove_output_observer(self, observer):
        """
        De-register output observer

        :param observer observer to de-register
        """


class ExecutionLifecycle:

    def __init__(self, *state_changes: Tuple[ExecutionState, datetime.datetime]):
        self._state_changes: OrderedDict[ExecutionState, datetime.datetime] = OrderedDict(state_changes)

    @classmethod
    def from_dict(cls, as_dict):
        state_changes = ((ExecutionState[state_change['state']], util.parse_datetime(state_change['changed']))
                         for state_change in as_dict['state_changes'])
        return cls(*state_changes)

    @property
    def state(self):
        return next(reversed(self._state_changes.keys()), ExecutionState.NONE)

    @property
    def states(self) -> List[ExecutionState]:
        return list(self._state_changes.keys())

    @property
    def state_changes(self) -> Iterable[Tuple[ExecutionState, datetime.datetime]]:
        return ((state, changed) for state, changed in self._state_changes.items())

    def changed_at(self, state: ExecutionState) -> datetime.datetime:
        return self._state_changes[state]

    @property
    def last_changed_at(self) -> Optional[datetime.datetime]:
        return next(reversed(self._state_changes.values()), None)

    @property
    def created_at(self) -> datetime.datetime:
        return self.changed_at(ExecutionState.CREATED)

    @property
    def first_executing_state(self) -> Optional[ExecutionState]:
        return next((state for state in self._state_changes if state.is_executing()), None)

    def executed(self) -> bool:
        return self.first_executing_state is not None

    @property
    def executed_at(self) -> Optional[datetime.datetime]:
        return self._state_changes.get(self.first_executing_state)

    @property
    def ended_at(self) -> Optional[datetime.datetime]:
        state = self.state
        if not state.is_terminal():
            return None
        return self.changed_at(state)

    @property
    def execution_time(self) -> Optional[datetime.timedelta]:
        start = self.executed_at
        if not start:
            return None

        end = self.ended_at or util.utc_now()
        return end - start

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        d = {
            "state_changes": [{"state": state.name, "changed": format_dt_iso(change)} for state, change in
                              self.state_changes],
            "state": self.state.name,
            "last_changed_at": format_dt_iso(self.last_changed_at),
            "created_at": format_dt_iso(self.created_at),
            "executed_at": format_dt_iso(self.executed_at),
            "ended_at": format_dt_iso(self.ended_at),
            "execution_time": self.execution_time.total_seconds() if self.executed_at else None,
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    def __copy__(self):
        copied = ExecutionLifecycle()
        copied._state_changes = self._state_changes
        return copied

    def __deepcopy__(self, memo):
        return ExecutionLifecycle(*self.state_changes)

    def __eq__(self, other):
        if not isinstance(other, ExecutionLifecycle):
            return NotImplemented
        return self._state_changes == other._state_changes

    def __hash__(self):
        return hash(tuple(self._state_changes.items()))

    def __repr__(self) -> str:
        return "{}({!r})".format(
            self.__class__.__name__, self._state_changes)


class ExecutionLifecycleManagement(ExecutionLifecycle):

    def __init__(self, *state_changes: Tuple[ExecutionState, datetime.datetime]):
        super().__init__(*state_changes)

    def set_state(self, new_state) -> bool:
        if not new_state or new_state == ExecutionState.NONE or self.state == new_state:
            return False
        else:
            self._state_changes[new_state] = utc_now()
            return True


class ExecutionOutputObserver(abc.ABC):

    def execution_output_update(self, output, is_error: bool):
        """
        Executed when new output line is available

        :param output: output text
        :param is_error: True when the text represents error output
        """


class ExecutionOutputTracker(ExecutionOutputObserver):

    def __init__(self, output_tracker):
        self.output_tracker = output_tracker

    def execution_output_update(self, output, is_error: bool):
        self.output_tracker.new_output(output)
