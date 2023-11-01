"""
This module defines parts of the execution framework, which provide an abstraction for executing a task.
During its lifecycle, an execution is expected to transition through various execution states, which are defined
by the `ExecutionState` enum. Each state belongs to a single phase represented by the `ExecutionPhase` enum and
can be associated with multiple execution flags, represented by the `ExecutionStateFlag` enum.
The flags can be viewed as attributes that characterize the state.

TODO: Remove execution prefix where appropriate
"""

import abc
from typing import Tuple

from tarotools.taro.run import TerminationStatus, PhaseStep, Phase, RunState


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
    def execute(self) -> TerminationStatus:
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
    def add_output_callback(self, callback):
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
    def remove_output_callback(self, callback):
        """
        De-register the output callback function

        Args:
            callback (Callable[[str, bool], None]): The callback to de-register
        """


class ExecutingPhase(PhaseStep):

    def __init__(self, phase_name, execution):
        super().__init__(Phase(phase_name, RunState.EXECUTING))
        self._execution = execution

    @property
    def stop_status(self):
        return TerminationStatus.STOPPED

    def run(self):
        self._execution.execute()

    def stop(self):
        self._execution.stop()
