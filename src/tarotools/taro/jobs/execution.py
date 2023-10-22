"""
This module defines parts of the execution framework, which provide an abstraction for executing a task.
During its lifecycle, an execution is expected to transition through various execution states, which are defined
by the `ExecutionState` enum. Each state belongs to a single phase represented by the `ExecutionPhase` enum and
can be associated with multiple execution flags, represented by the `ExecutionStateFlag` enum.
The flags can be viewed as attributes that characterize the state.

TODO: Remove execution prefix where appropriate
"""

import abc
from enum import Enum, auto, EnumMeta
from typing import Tuple, Set, Dict, Any

from tarotools.taro.jobs.instance import InstancePhase
from tarotools.taro.util import is_empty
from tarotools.taro.util.observer import Notification

Phase = InstancePhase


class TerminationStatusFlag(Enum):
    BEFORE_EXECUTION = auto()  # Not yet executed
    UNEXECUTED = auto()  # Not yet executed or reached terminal phase without execution.
    WAITING = auto()     # Waiting for a condition before execution.
    DISCARDED = auto()   # Discarded automatically or by the user before execution.
    REJECTED = auto()    # Automatically rejected before execution.
    EXECUTED = auto()    # Reached the executing state.
    SUCCESS = auto()     # Completed successfully.
    NONSUCCESS = auto()  # Not completed successfully either before or after execution.
    INCOMPLETE = auto()  # Not completed successfully after execution.
    FAILURE = auto()     # Failed after or before execution.
    ABORTED = auto()     # Interrupted by the user after or before execution.


Flag = TerminationStatusFlag


class TerminationStatusMeta(EnumMeta):
    def __getitem__(self, name):
        if not name:
            return TerminationStatus.NONE
        try:
            return super().__getitem__(name.upper())
        except KeyError:
            return TerminationStatus.UNKNOWN


class TerminationStatus(Enum, metaclass=TerminationStatusMeta):
    NONE =    Phase.NONE, {}
    UNKNOWN = Phase.NONE, {}

    CREATED = Phase.CREATED, {Flag.BEFORE_EXECUTION, Flag.UNEXECUTED}

    PENDING = Phase.PENDING, {Flag.BEFORE_EXECUTION, Flag.UNEXECUTED, Flag.WAITING}  # Until released
    QUEUED =  Phase.QUEUED, {Flag.BEFORE_EXECUTION, Flag.UNEXECUTED, Flag.WAITING}  # Wait for another job

    CANCELLED =   Phase.TERMINAL, {Flag.UNEXECUTED, Flag.NONSUCCESS, Flag.DISCARDED, Flag.ABORTED}
    SKIPPED =     Phase.TERMINAL, {Flag.UNEXECUTED, Flag.NONSUCCESS, Flag.DISCARDED, Flag.REJECTED}
    UNSATISFIED = Phase.TERMINAL, {Flag.UNEXECUTED, Flag.NONSUCCESS, Flag.DISCARDED, Flag.REJECTED}
    # More possible discarded states: DISABLED, SUSPENDED

    # START_FAILED = {ExecutionStateGroup.TERMINAL, ExecutionStateGroup.UNEXECUTED, ExecutionStateGroup.FAILURE}
    # TRIGGERED = Phase.EXECUTING, {Flag.EXECUTED}  # Start request sent, start confirmation not (yet) received
    # STARTED =   {ExecutionStateGroup.EXECUTED, ExecutionStateGroup.EXECUTING}
    RUNNING = Phase.EXECUTING, {Flag.EXECUTED}

    COMPLETED =   Phase.TERMINAL, {Flag.EXECUTED, Flag.SUCCESS}

    STOPPED =     Phase.TERMINAL, {Flag.EXECUTED, Flag.NONSUCCESS, Flag.INCOMPLETE, Flag.ABORTED}
    INTERRUPTED = Phase.TERMINAL, {Flag.EXECUTED, Flag.NONSUCCESS, Flag.INCOMPLETE, Flag.ABORTED}
    FAILED =      Phase.TERMINAL, {Flag.EXECUTED, Flag.NONSUCCESS, Flag.INCOMPLETE, Flag.FAILURE}  # TODO Remove EXECUTED
    ERROR =       Phase.TERMINAL, {Flag.EXECUTED, Flag.NONSUCCESS, Flag.INCOMPLETE, Flag.FAILURE}  # TODO Remove EXECUTED

    def __new__(cls, *args, **kwds):
        value = len(cls.__members__) + 1
        obj = object.__new__(cls)
        obj._value_ = value
        return obj

    @classmethod
    def with_flags(cls, *flags):
        return [state for state in cls if all(flag in state.flags for flag in flags)]

    def __init__(self, phase: InstancePhase, groups: Set[TerminationStatusFlag]):
        self.phase = phase
        self.flags = groups

    def in_phase(self, phase: InstancePhase) -> bool:
        return self.phase == phase

    def has_flag(self, flag: TerminationStatusFlag):
        return flag in self.flags


class UnexpectedStateError(Exception):
    """
    Raised when processing logic encounters an unrecognized or invalid
    execution state in the given context.
    """


class ExecutionError(Exception):
    """
    This exception is used to provide additional information about an error condition that occurred during execution.
    """

    @classmethod
    def from_unexpected_error(cls, e: Exception):
        return cls("Unexpected error: " + str(e), TerminationStatus.ERROR, unexpected_error=e)

    @classmethod
    def from_dict(cls, as_dict):
        return cls(as_dict['message'], TerminationStatus[as_dict['state']])  # TODO Add kwargs

    def __init__(self, message: str, exec_state: TerminationStatus, unexpected_error: Exception = None, **kwargs):
        if not exec_state.has_flag(Flag.FAILURE):
            raise ValueError('exec_state must be flagged as failure', exec_state)
        super().__init__(message)
        self.message = message
        self.exec_state = exec_state
        self.unexpected_error = unexpected_error
        self.params = kwargs

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        # TODO Add kwargs
        d = {
            "message": self.message,
            "state": self.exec_state.name,
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    def __hash__(self):
        """
        Used for comparing in tests
        """
        return hash((self.message, self.exec_state, self.unexpected_error, self.params))

    def __eq__(self, other):
        """
        Used for comparing in tests
        """
        if not isinstance(other, ExecutionError):
            return NotImplemented
        return (self.message, self.exec_state, self.unexpected_error, self.params) == (
            other.message, other.exec_state, other.unexpected_error, self.params)


class Execution(abc.ABC):
    """
    A synchronous execution of a task
    """

    @abc.abstractmethod
    def execute(self) -> TerminationStatus:
        """
        For the caller of this method:
            This execution instance must be in `Phase.EXECUTING` phase when this method is called.
            This execution instance must be in `Phase.TERMINAL` phase when this method returns a value.

        For the implementer of this class:
            The execution must be started when this method is called.
            The returned value must be a terminal execution state representing the final state of the execution.
            In case of a failure an execution error can be raised or a failure state can be returned.

        Raises:
            ExecutionError: To provide more information when a failure or an error occurred during the execution.
        """

    @property
    @abc.abstractmethod
    def tracking(self):
        """
        Returns:
            An object containing tracking information about the progress of the execution
        """

    @property
    @abc.abstractmethod
    def status(self):
        """
        Gets the status of the progress.

        If progress monitoring is not supported, this method will always return None. Otherwise:
         - if executing: returns the current progress.
         - when finished: returns the result.

        Returns:
            str: The progress or result if applicable, or None if progress monitoring is not supported.
        """

    @property
    @abc.abstractmethod
    def parameters(self):
        """
        Returns:
            Tuple[str, str]: A sequence representing arbitrary immutable execution parameters
        """

    @abc.abstractmethod
    def stop(self):
        """
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
    """
    An execution which produces output.
    """

    @abc.abstractmethod
    def add_output_observer(self, observer):
        """
        Register output observer

        Args:
            observer: to register
        """

    @abc.abstractmethod
    def remove_output_observer(self, observer):
        """
        De-register output observer

        Args:
            observer: to de-register
        """


class ExecutionOutputObserver(abc.ABC):

    def execution_output_update(self, output, is_error: bool):
        """
        Executed when a new output line is available.

        Args:
            output (str): The output text.
            is_error (bool): True when the text represents an error output.
        """


class ExecutionOutputNotification(Notification):

    def __init__(self, logger=None, joined_notification=None):
        super().__init__(logger, joined_notification)

    def _notify(self, observer, *args) -> bool:
        if isinstance(observer, ExecutionOutputObserver):
            observer.execution_output_update(*args)
            return True
        else:
            return False
