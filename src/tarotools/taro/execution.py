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
from tarotools.taro.util.observer import CallableNotification


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


class ExecutionOutputNotification(CallableNotification):

    def __init__(self, error_hook=None, joined_notification=None):
        super().__init__(error_hook, joined_notification)

    def _notify(self, observer, *args) -> bool:
        if isinstance(observer, ExecutionOutputObserver):
            observer.execution_output_update(*args)
            return True
        else:
            return False


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
