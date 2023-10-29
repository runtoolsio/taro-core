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
from tarotools.taro.util.observer import Notification


class UnexpectedStateError(Exception):
    """
    Raised when processing logic encounters an unrecognized or invalid
    execution state in the given context.
    """


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


class ExecutingPhase(PhaseStep):

    def __init__(self, phase_name, execution):
        self._phase_name = phase_name
        self._execution = execution

    @property
    def phase(self):
        return Phase(self._phase_name, RunState.EXECUTING)

    def run(self):
        self._execution.execute()

    def stop(self):
        self._execution.stop()

    @property
    def stop_status(self):
        return TerminationStatus.STOPPED
