import subprocess
import sys

from taro.execution import Execution, ExecutionState, ExecutionError


class ProcessExecution(Execution):

    def __init__(self, args):
        self.args = args

    def execute(self) -> ExecutionState:
        try:
            ret_code = subprocess.call(self.args)
            return ExecutionState.COMPLETED
        except FileNotFoundError as e:
            sys.stderr.write(str(e) + "\n")
            raise ExecutionError(str(e), ExecutionState.FAILED) from e
