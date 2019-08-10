import abc
from enum import Enum


# TODO STOPPED
class ExecutionState(Enum):
    NONE = 0
    TRIGGERED = 1
    STARTED = 2
    COMPLETED = 3
    NOT_STARTED = 4
    FAILED = 5
    ERROR = 6

    def is_terminal(self) -> bool:
        return self.value >= 3

    def is_failure(self) -> bool:
        return self.value >= 4


class ExecutionError(Exception):

    def __init__(self, message: str, exec_state: ExecutionState, unexpected_error: Exception = None, **kwargs):
        if not exec_state.is_failure():
            raise ValueError('exec_state must be a failure', exec_state)
        super().__init__(message)
        self.message = message
        self.exec_state = exec_state
        self.unexpected_error = unexpected_error
        self.params = kwargs


class Execution(abc.ABC):

    def execute_catch_exc(self) -> ExecutionState:
        try:
            return self.execute()
        except ExecutionError:
            raise
        except Exception as e:
            raise ExecutionError("Unexpected error", ExecutionState.ERROR, unexpected_error=e)

    @abc.abstractmethod
    def execute(self) -> ExecutionState:
        """
        Executes job

        :return: a state of the job instance after the execution of this method
        :raises ExecutionError: when execution failed due to known cause
        :raises Exception: on unexpected failure
        """
