"""
Execution framework defines an abstraction for an execution of a task.
It consists of:
 1. Possible states of execution
 2. A structure for conveying of error conditions
 3. An interface for implementing various types of executions
"""

import abc
from enum import Enum


# TODO INIT_FAILED
class ExecutionState(Enum):
    NONE = 0
    TRIGGERED = 1
    STARTED = 2
    COMPLETED = 3
    STOPPED = 4
    START_FAILED = 5
    INTERRUPTED = 6
    FAILED = 7
    ERROR = 8

    def is_terminal(self) -> bool:
        return self.value >= 4

    def is_failure(self) -> bool:
        return self.value >= 5


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
    def stop_execution(self):
        """
        Stop running execution
        """
