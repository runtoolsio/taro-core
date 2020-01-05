from subprocess import Popen
from typing import Union

import sys

from taro.execution import Execution, ExecutionState, ExecutionError


class ProcessExecution(Execution):

    def __init__(self, args):
        self.args = args
        self.popen: Union[Popen, None] = None
        self.stopped = False
        self.interrupt_signal = 0

    def execute(self) -> ExecutionState:
        ret_code = -1
        if not self.stopped and self.interrupt_signal == 0:
            try:
                self.popen = Popen(self.args)
                ret_code = self.popen.wait()
                if ret_code == 0:
                    return ExecutionState.COMPLETED
            except FileNotFoundError as e:
                sys.stderr.write(str(e) + "\n")
                raise ExecutionError(str(e), ExecutionState.FAILED) from e
            except SystemExit as e:
                raise ExecutionError(str(e), ExecutionState.INTERRUPTED) from e

        if self.stopped:
            return ExecutionState.STOPPED
        elif self.interrupt_signal > 0:
            raise ExecutionError("Interrupted by signal:" + str(self.interrupt_signal), ExecutionState.INTERRUPTED)
        else:
            raise ExecutionError("Process returned non-zero code: " + str(ret_code), ExecutionState.FAILED)

    def stop_execution(self):
        self.stopped = True
        if self.popen:
            self.popen.terminate()

    def interrupt(self, signal):
        if signal <= 0:
            raise ValueError('Signal value must be greater than zero but was: ' + str(signal))
        self.interrupt_signal = signal
        self.popen.terminate()
