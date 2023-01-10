"""
TODO: Create option where the command will terminates if the specified state is found in the previous or current state
      of an existing instance.
"""

import signal
import sys

from taro.jobs.job import ExecutionStateObserver, JobInfo
from taro.listening import StateReceiver
from taro.util import MatchingStrategy
from taroapp import printer, style
from taroapp.cmd import cliutil


def run(args):
    instance_match = cliutil.instance_matching_criteria(args, MatchingStrategy.PARTIAL)
    receiver = StateReceiver(instance_match, args.states)
    receiver.listeners.append(EventHandler(receiver, args.count))
    signal.signal(signal.SIGTERM, lambda _, __: _close_server_and_exit(receiver, signal.SIGTERM))
    signal.signal(signal.SIGINT, lambda _, __: _close_server_and_exit(receiver, signal.SIGINT))
    receiver.start()


def _close_server_and_exit(server, signal_number: int):
    server.close()
    sys.exit(128 + signal_number)


def print_state_change(job_info):
    printer.print_styled(*style.job_status_line_styled(job_info))


class EventHandler(ExecutionStateObserver):

    def __init__(self, closeable, count=1):
        self._closeable = closeable
        self.count = count

    def state_update(self, job_info: JobInfo):
        try:
            print_state_change(job_info)
        except BrokenPipeError:
            self._closeable.close()
            cliutil.handle_broken_pipe(exit_code=1)

        self.count -= 1
        if self.count <= 0:
            self._closeable.close()
