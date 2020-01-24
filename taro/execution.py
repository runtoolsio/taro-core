"""
Execution framework defines an abstraction for an execution of a task.
It consists of:
 1. Possible states of execution
 2. A structure for conveying of error conditions
 3. An interface for implementing various types of executions
"""

import abc
from enum import IntEnum


class ExecutionState(IntEnum):
    NONE = 0
    CREATED = 1
    WAITING = 2
    TRIGGERED = 3
    STARTED = 4
    COMPLETED = 5
    CANCELLED = 6
    STOPPED = 7
    START_FAILED = 8
    INTERRUPTED = 9
    FAILED = 10
    ERROR = 11

    def is_executing(self):
        return ExecutionState.TRIGGERED <= self <= ExecutionState.STARTED

    def is_terminal(self) -> bool:
        return self >= ExecutionState.COMPLETED

    def is_failure(self) -> bool:
        return self >= ExecutionState.START_FAILED


class ExecutionError(Exception):

    @classmethod
    def from_unexpected_error(cls, e: Exception):
        return cls("Unexpected error", ExecutionState.ERROR, unexpected_error=e)

    def __init__(self, message: str, exec_state: ExecutionState, unexpected_error: Exception = None, **kwargs):
        if not exec_state.is_failure():
            raise ValueError('exec_state must be a failure', exec_state)
        super().__init__(message)
        self.message = message
        self.exec_state = exec_state
        self.unexpected_error = unexpected_error
        self.params = kwargs


class Execution(abc.ABC):

    @abc.abstractmethod
    def execute(self) -> ExecutionState:
        """
        Executes a task

        :return: state after the execution of this method
        :raises ExecutionError: when execution failed
        :raises Exception: on unexpected failure
        """

    @abc.abstractmethod
    def stop(self):
        """
        If not yet executed: Do not execute
        If already executing: Stop running execution GRACEFULLY
        If execution finished: Ignore
        """

    @abc.abstractmethod
    def interrupt(self):
        """
        If not yet executed: Do not execute
        If already executing: Stop running execution IMMEDIATELY
        If execution finished: Ignore
        """
