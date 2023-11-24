"""
This module defines parts of the execution framework, which provide an abstraction for executing a task.
During its lifecycle, an execution is expected to transition through various execution states, which are defined
by the `ExecutionState` enum. Each state belongs to a single phase represented by the `ExecutionPhase` enum and
can be associated with multiple execution flags, represented by the `ExecutionStateFlag` enum.
The flags can be viewed as attributes that characterize the state.

TODO: Remove execution prefix where appropriate
"""

import abc
from enum import auto
from typing import Tuple

from tarotools.taro.run import TerminationStatus, Phase, RunState, TerminateRun, FailedRun


class ExecutionException(Exception):
    pass


class ExecutionResult:
    DONE = auto()
    STOPPED = auto()
    INTERRUPTED = auto()


class Execution(abc.ABC):
    """
    An execution of a task
    """

    @property
    @abc.abstractmethod
    def parameters(self):
        """
        Returns:
            Tuple[str, str]: A sequence representing arbitrary immutable execution parameters
        """

    @abc.abstractmethod
    def execute(self) -> ExecutionResult:
        """
        Execute a task and return its termination status.
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
    def add_callback_output(self, callback):
        """
        Register a function to be called on output notification.

        The expected function signature is:
            callback(output: str, is_error: bool) -> None

        This means the callback should accept two arguments:
        - `output`: The actual output data.
        - `is_error`: A boolean indicating whether the output represents an error.

        Args:
            callback (Callable[[str, bool], None]): The function to register as a callback.
        """

    @abc.abstractmethod
    def remove_callback_output(self, callback):
        """
        De-register the output callback function

        Args:
            callback (Callable[[str, bool], None]): The callback to de-register
        """


class ExecutingPhase(Phase):

    def __init__(self, phase_name, execution):
        super().__init__(phase_name, RunState.EXECUTING)
        self._execution = execution

    @property
    def execution(self):
        return self._execution

    @property
    def stop_status(self):
        return TerminationStatus.STOPPED

    def run(self):
        if hasattr(self._execution, 'add_callback_output'):
            self._execution.add_callback_output(self._output_notification)

        try:
            exec_res = self._execution.execute()
        except ExecutionException as e:
            raise FailedRun('ExecutionException', str(e))
        finally:
            if hasattr(self._execution, 'remove_callback_output'):
                self._execution.remove_callback_output(self._output_notification)

        match exec_res:
            case ExecutionResult.STOPPED:
                raise TerminateRun(TerminationStatus.STOPPED)
            case ExecutionResult.INTERRUPTED:
                raise TerminateRun(TerminationStatus.INTERRUPTED)
            case _:
                pass

    def stop(self):
        self._execution.stop()
