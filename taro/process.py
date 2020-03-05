import sys
from subprocess import Popen
from typing import Union

from taro.execution import Execution, ExecutionState, ExecutionError


class ProcessExecution(Execution):

    def __init__(self, args, progress: bool):
        self.args = args
        self.progress: bool = progress
        self.popen: Union[Popen, None] = None
        self._stopped: bool = False
        self._interrupted: bool = False

    def is_async(self) -> bool:
        return False

    def execute(self) -> ExecutionState:
        ret_code = -1
        if not self._stopped and not self._interrupted:
            try:
                self.popen = Popen(self.args, stdout=None)

                # print(psutil.Process(self.popen.pid).memory_info().rss)

                ret_code = self.popen.wait()
                if ret_code == 0:
                    return ExecutionState.COMPLETED
            except KeyboardInterrupt:
                return ExecutionState.STOPPED
            except FileNotFoundError as e:
                sys.stderr.write(str(e) + "\n")
                raise ExecutionError(str(e), ExecutionState.FAILED) from e
            except SystemExit as e:
                raise ExecutionError('System exit', ExecutionState.INTERRUPTED) from e

        if self._stopped:
            return ExecutionState.STOPPED
        if self._interrupted:
            raise ExecutionError("Process interrupted", ExecutionState.INTERRUPTED)
        raise ExecutionError("Process returned non-zero code " + str(ret_code), ExecutionState.FAILED)

    def progress(self):
        pass

    def stop(self):
        self._stopped = True
        if self.popen:
            self.popen.terminate()

    def interrupt(self):
        self._interrupted = True
        if self.popen:
            self.popen.terminate()
