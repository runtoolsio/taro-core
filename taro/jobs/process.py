import logging
import signal
import sys
import traceback
from contextlib import contextmanager
from multiprocessing import Queue
from multiprocessing.context import Process
from queue import Full
from threading import Thread
from typing import Union, Tuple

from taro import ExecutionState
from taro.jobs.execution import OutputExecution, ExecutionError, ExecutionOutputObserver

log = logging.getLogger(__name__)


class ProcessExecution(OutputExecution):

    def __init__(self, target, args=(), tracking=None):
        self.target = target
        self.args = args
        self._tracking = tracking
        self.output_queue: Queue[Tuple[Union[str, _QueueStop], bool]] = Queue(maxsize=2048)  # Create in execute method?
        self._process: Union[Process, None] = None
        self._status = None
        self._stopped: bool = False
        self._interrupted: bool = False
        self._output_observers = []

    @property
    def is_async(self) -> bool:
        return False

    def execute(self) -> ExecutionState:
        if not self._stopped and not self._interrupted:
            output_reader = Thread(target=self._read_output, name='Output-Reader', daemon=True)
            output_reader.start()
            self._process = Process(target=self._run)

            try:
                self._process.start()
                self._process.join()
            finally:
                self.output_queue.put_nowait((_QueueStop(), False))
                output_reader.join(timeout=1)
                self.output_queue.close()

            if self._process.exitcode == 0:
                return ExecutionState.COMPLETED

        if self._interrupted or self._process.exitcode == -signal.SIGINT:
            # Exit code is -SIGINT only when SIGINT handler is set back to DFL (KeyboardInterrupt gets exit code 1)
            return ExecutionState.INTERRUPTED
        if self._stopped or self._process.exitcode < 0:  # Negative exit code means terminated by a signal
            return ExecutionState.STOPPED
        raise ExecutionError("Process returned non-zero code " + str(self._process.exitcode), ExecutionState.FAILED)

    def _run(self):
        with self._capture_stdout():
            try:
                self.target(*self.args)
            except:
                for line in traceback.format_exception(*sys.exc_info()):
                    self.output_queue.put_nowait((line, True))
                raise

    @contextmanager
    def _capture_stdout(self):
        import sys
        original_stdout = sys.stdout
        original_stderr = sys.stderr
        stdout_writer = _CapturingWriter(original_stdout, False, self.output_queue)
        stderr_writer = _CapturingWriter(original_stderr, True, self.output_queue)
        sys.stdout = stdout_writer
        sys.stderr = stderr_writer

        try:
            yield
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr

    @property
    def tracking(self):
        return self._tracking

    @tracking.setter
    def tracking(self, tracking):
        self._tracking = tracking

    @property
    def status(self):
        if self.tracking:
            return self.tracking.status
        else:
            return self._status

    @property
    def parameters(self):
        return ('execution', 'process'),

    def stop(self):
        self._stopped = True
        if self._process:
            self._process.terminate()

    def interrupted(self):
        self._interrupted = True

    def add_output_observer(self, observer):
        self._output_observers.append(observer)

    def remove_output_observer(self, observer):
        self._output_observers.remove(observer)

    def _read_output(self):
        while True:
            output_text, is_err = self.output_queue.get()
            if isinstance(output_text, _QueueStop):
                break
            self._status = output_text
            self._notify_output_observers(output_text, is_err)

    def _notify_output_observers(self, output, is_err):
        for observer in self._output_observers:
            # noinspection PyBroadException
            try:
                if isinstance(observer, ExecutionOutputObserver):
                    observer.execution_output_update(output, is_err)
                elif callable(observer):
                    observer(output)
                else:
                    log.warning("event=[unsupported_output_observer] observer=[%s]", observer)
            except BaseException:
                log.exception("event=[state_observer_exception]")


class _CapturingWriter:

    def __init__(self, out, is_err, output_queue):
        self.out = out
        self.is_err = is_err
        self.output_queue = output_queue

    def write(self, text):
        text_s = text.rstrip()
        if text_s:
            try:
                self.output_queue.put_nowait((text_s, self.is_err))
            except Full:
                # TODO what to do here?
                log.warning("event=[output_queue_full]")
        self.out.write(text)


class _QueueStop:
    """Poison object signalizing no more objects will be put in the queue"""
    pass
