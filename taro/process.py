import subprocess

from taro.execution import Execution, ExecutionState, ExecutionError


class ProcessExecution(Execution):

    def __init__(self, args):
        self.args = args

    def execute(self) -> ExecutionState:
        try:
            subprocess.call(self.args)
            return ExecutionState.COMPLETED
        except FileNotFoundError as e:
            raise ExecutionError(str(e), ExecutionState.FAILED) from e
