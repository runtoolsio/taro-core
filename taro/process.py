from subprocess import Popen
from typing import Union

import sys

from taro.execution import Execution, ExecutionState, ExecutionError


class ProcessExecution(Execution):

    def __init__(self, args):
        self.args = args
        self.popen: Union[Popen, None] = None
        self.interrupt_signal = -1

    def execute(self) -> ExecutionState:
        ret_code = -1
        if self.interrupt_signal == -1:
            try:
                self.popen = Popen(self.args)
                ret_code = self.popen.wait()
            except FileNotFoundError as e:
                sys.stderr.write(str(e) + "\n")
                raise ExecutionError(str(e), ExecutionState.FAILED) from e
            except SystemExit as e:
                raise ExecutionError(str(e), ExecutionState.INTERRUPTED) from e

        if ret_code == 0:
            return ExecutionState.COMPLETED
        elif self.interrupt_signal == 0:
            return ExecutionState.STOPPED
        elif self.interrupt_signal > 0:
            raise ExecutionError("Interrupted by signal:" + str(self.interrupt_signal), ExecutionState.INTERRUPTED)
        else:
            raise ExecutionError("Process returned non-zero code: " + str(ret_code), ExecutionState.FAILED)

    def stop_execution(self):
        self.interrupt_signal = 0
        if self.popen:
            self.popen.terminate()
