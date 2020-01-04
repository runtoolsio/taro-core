import subprocess

import sys

from taro.execution import Execution, ExecutionState, ExecutionError


class ProcessExecution(Execution):

    def __init__(self, args):
        self.args = args

    def execute(self) -> ExecutionState:
        try:
            ret_code = subprocess.call(self.args)
            if ret_code == 0:
                return ExecutionState.COMPLETED
            else:
                raise ExecutionError("Process returned non-zero code: " + str(ret_code), ExecutionState.FAILED)
        except FileNotFoundError as e:
            sys.stderr.write(str(e) + "\n")
            raise ExecutionError(str(e), ExecutionState.FAILED) from e
        except SystemExit as e:
            raise ExecutionError(str(e), ExecutionState.STOPPED) from e
