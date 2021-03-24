from contextlib import contextmanager
from multiprocessing.context import Process
from multiprocessing import Queue
from typing import Union

from taro import ExecutionState
from taro.execution import OutputExecution, ExecutionError


class ProcessExecution(OutputExecution):

    def __init__(self, target, args):
        self.target = target
        self.args = args
        self.output_queue = Queue()
        self._process: Union[Process, None] = None
        self._stopped: bool = False
        self._interrupted: bool = False

    def is_async(self) -> bool:
        return False

    def execute(self) -> ExecutionState:
        if not self._stopped and not self._interrupted:
            self._process = Process(target=self._run)
            self._process.start()
            self._process.join()
            if self._process.exitcode == 0:
                return ExecutionState.COMPLETED
        if self._stopped:
            return ExecutionState.STOPPED
        if self._interrupted:
            raise ExecutionError("Process interrupted", ExecutionState.INTERRUPTED)
        raise ExecutionError("Process returned non-zero code " + str(self._process.exitcode), ExecutionState.FAILED)

    def _run(self):
        with self._capture_stdout():
            self.target(*self.args)

    @contextmanager
    def _capture_stdout(self):
        import sys
        original_stdout = sys.stdout
        writer = _CapturingWriter(original_stdout, self)
        sys.stdout = writer

        try:
            yield
        finally:
            sys.stdout = original_stdout

    def status(self):
        pass

    def stop(self):
        self._stopped = True
        if self._process:
            self._process.terminate()

    def interrupt(self):
        self._interrupted = True
        if self._process:
            self._process.terminate()

    def add_output_observer(self, observer):
        pass

    def remove_output_observer(self, observer):
        pass


class _CapturingWriter:

    def __init__(self, out, exec_obj):
        self.out = out
        self.exec_obj = exec_obj

    def write(self, text):
        text_s = text.rstrip()
        if text_s:
            self.exec_obj.output_queue.put(text_s)
        self.out.write(text)
