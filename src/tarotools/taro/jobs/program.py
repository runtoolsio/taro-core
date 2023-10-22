"""
This module contains the `ProgramExecution` class, an implementation of the `Execution` abstract class, used to
run an external program using the `subprocess` module from the standard library.
"""

import io
import logging
import signal
import sys
from subprocess import Popen, PIPE
from threading import Thread
from typing import Union, Optional

from tarotools.taro.jobs.execution import TerminationStatus, ExecutionError, OutputExecution, \
    ExecutionOutputNotification

USE_SHELL = False  # For testing only

log = logging.getLogger(__name__)


class ProgramExecution(OutputExecution):

    def __init__(self, *args, read_output: bool = True, tracking=None):
        self.args = args
        self.read_output: bool = read_output
        self._tracking = tracking
        self._popen: Union[Popen, None] = None
        self._status = None
        self._stopped: bool = False
        self._interrupted: bool = False
        self._output_observers = ExecutionOutputNotification(log)

    @property
    def ret_code(self) -> Optional[int]:
        if self._popen is None:
            return None

        return self._popen.returncode

    def execute(self) -> TerminationStatus:
        if not self._stopped and not self._interrupted:
            stdout = PIPE if self.read_output else None
            stderr = PIPE if self.read_output else None
            try:
                self._popen = Popen(" ".join(self.args) if USE_SHELL else self.args, stdout=stdout, stderr=stderr,
                                    shell=USE_SHELL)
                stdout_reader = None
                stderr_reader = None
                if self.read_output:
                    stdout_reader = self._start_output_reader(self._popen.stdout, False)
                    stderr_reader = self._start_output_reader(self._popen.stderr, True)

                self._popen.wait()
                if self.read_output:
                    stdout_reader.join(timeout=1)
                    stderr_reader.join(timeout=1)
                if self.ret_code == 0:
                    return TerminationStatus.COMPLETED
            except FileNotFoundError as e:
                sys.stderr.write(str(e) + "\n")
                raise ExecutionError(str(e), TerminationStatus.FAILED) from e

        if self._interrupted or self.ret_code == -signal.SIGINT:
            return TerminationStatus.INTERRUPTED
        if self._stopped or self.ret_code < 0:  # Negative exit code means terminated by a signal
            return TerminationStatus.STOPPED
        raise ExecutionError("Process returned non-zero code " + str(self.ret_code), TerminationStatus.FAILED)

    def _start_output_reader(self, infile, is_err):
        name = 'Stderr-Reader' if is_err else 'Stdout-Reader'
        t = Thread(target=self._process_output, args=(infile, is_err), name=name, daemon=True)
        t.start()
        return t

    @property
    def tracking(self):
        return self._tracking

    @tracking.setter
    def tracking(self, tracking):
        self._tracking = tracking

    @property
    def status(self):
        if self.tracking:
            return str(self.tracking)
        else:
            return self._status

    @property
    def parameters(self):
        return ('execution', 'program'),

    def stop(self):
        self._stopped = True
        if self._popen:
            self._popen.terminate()

    def interrupted(self):
        """
        Call this if the execution was possibly interrupted externally (Ctrl+C) to set the correct final state.
        On windows might be needed to send a signal?
        """
        self._interrupted = True

    def add_output_observer(self, observer):
        self._output_observers.add_observer(observer)

    def remove_output_observer(self, observer):
        self._output_observers.remove_observer(observer)

    def _process_output(self, infile, is_err):
        with infile:
            for line in io.TextIOWrapper(infile, encoding="utf-8"):
                line_stripped = line.rstrip()
                self._status = line_stripped
                print(line_stripped, file=sys.stderr if is_err else sys.stdout)
                self._output_observers.notify_all(line_stripped, is_err)
