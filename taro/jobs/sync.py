from abc import ABC, abstractmethod

from taro import ExecutionState


class Sync(ABC):

    @abstractmethod
    def current_state(self) -> ExecutionState:
        """
        If 'NONE' state is returned then the job can proceed with the execution.
        If returned state belongs to the 'TERMINAL' group then the job must be terminated.
        If returned state belongs to the 'WAITING' group then the job is obligated to call :func:`wait_and_release`.

        :return: execution state for job
        """

    @abstractmethod
    def wait_and_release(self, state_lock):
        """

        :param state_lock:
        :return:
        """
