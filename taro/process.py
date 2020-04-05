import io
import sys
from subprocess import Popen, PIPE
from threading import Thread
from typing import Union

from taro.execution import Execution, ExecutionState, ExecutionError

USE_SHELL = False  # For testing only


class ProcessExecution(Execution):

    def __init__(self, args, read_progress: bool):
        self.args = args
        self.read_progress: bool = read_progress
        self._popen: Union[Popen, None] = None
        self._progress = None
        self._stopped: bool = False
        self._interrupted: bool = False

    def is_async(self) -> bool:
        return False

    def execute(self) -> ExecutionState:
        ret_code = -1
        if not self._stopped and not self._interrupted:
            stdout = PIPE if self.read_progress else None
            try:
                self._popen = Popen(" ".join(self.args) if USE_SHELL else self.args, stdout=stdout, shell=USE_SHELL)
                if self.read_progress:
                    Thread(target=self._read_progress, name='Progress-Reader', daemon=True).start()

                # print(psutil.Process(self.popen.pid).memory_info().rss)

                ret_code = self._popen.wait()
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
        return self._progress

    def _read_progress(self):
        for line in io.TextIOWrapper(self._popen.stdout, encoding="utf-8"):
            self._progress = line.rstrip()
            print(self._progress)

    def stop(self):
        self._stopped = True
        if self._popen:
            self._popen.terminate()

    def interrupt(self):
        self._interrupted = True
        if self._popen:
            self._popen.terminate()
