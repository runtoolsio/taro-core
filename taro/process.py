import io
import sys
from subprocess import Popen, PIPE
from threading import Thread
from typing import Union

from taro.execution import Execution, ExecutionState, ExecutionError

USE_SHELL = False  # For testing only


class ProcessExecution(Execution):

    def __init__(self, args, read_output: bool):
        self.args = args
        self.read_output: bool = read_output
        self._popen: Union[Popen, None] = None
        self._status = None
        self._stopped: bool = False
        self._interrupted: bool = False

    def is_async(self) -> bool:
        return False

    def execute(self) -> ExecutionState:
        ret_code = -1
        if not self._stopped and not self._interrupted:
            stdout = PIPE if self.read_output else None
            try:
                self._popen = Popen(" ".join(self.args) if USE_SHELL else self.args, stdout=stdout, shell=USE_SHELL)
                output_reader = None
                if self.read_output:
                    output_reader = Thread(target=self._read_output, name='Output-Reader', daemon=True)
                    output_reader.start()

                # print(psutil.Process(self.popen.pid).memory_info().rss)

                ret_code = self._popen.wait()
                if output_reader:
                    output_reader.join(timeout=1)
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

    def status(self):
        return self._status

    def _read_output(self):
        for line in io.TextIOWrapper(self._popen.stdout, encoding="utf-8"):
            self._status = line.rstrip()
            print(self._status)

    def stop(self):
        self._stopped = True
        if self._popen:
            self._popen.terminate()

    def interrupt(self):
        self._interrupted = True
        if self._popen:
            self._popen.terminate()
