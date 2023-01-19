import os
import signal
import sys
from typing import Callable, Sequence


def handle_broken_pipe(*, exit_code):
    # According to the official Python doc: https://docs.python.org/3/library/signal.html#note-on-sigpipe
    # Python flushes standard streams on exit; redirect remaining output
    # to devnull to avoid another BrokenPipeError at shutdown
    devnull = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull, sys.stdout.fileno())
    sys.exit(exit_code)  # Python exits with error code 1 on EPIPE


def exit_on_signal(*, cleanups: Sequence[Callable[[], None]] = (), print_signal=False):
    handler = SignalHandler(cleanups=cleanups, print_signal=print_signal)
    signal.signal(signal.SIGTERM, handler.terminate)
    signal.signal(signal.SIGINT, handler.interrupt)


class SignalHandler:

    def __init__(self, *, cleanups: Sequence[Callable[[], None]] = (), print_signal=False):
        self.cleanups = cleanups
        self.print_signal = print_signal

    def terminate(self, _, __):
        self._cleanup()
        if self.print_signal:
            print('event=[terminated_by_signal]')
        sys.exit(143)

    def interrupt(self, _, __):
        self._cleanup()
        if self.print_signal:
            print('event=[interrupted_by_signal]')
        sys.exit(130)

    def _cleanup(self):
        for c in self.cleanups:
            c()
