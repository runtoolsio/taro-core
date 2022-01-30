"""
TODO: Create option where the command will terminates if the specified state is found in the previous or current state
      of an existing instance.
"""

import signal
import sys

from taro.jobs.job import ExecutionStateObserver, JobInfo
from taro.listening import StateReceiver
from taroapp import printer, style


def run(args):
    receiver = StateReceiver(args.instance, args.states)
    receiver.listeners.append(lambda job_info: print_state_change(job_info))
    receiver.listeners.append(StoppingListener(receiver, args.count))
    signal.signal(signal.SIGTERM, lambda _, __: _close_server_and_exit(receiver, signal.SIGTERM))
    signal.signal(signal.SIGINT, lambda _, __: _close_server_and_exit(receiver, signal.SIGINT))
    receiver.start()


def _close_server_and_exit(server, signal_number: int):
    server.close()
    sys.exit(128 + signal_number)


def print_state_change(job_info):
    printer.print_styled(*style.job_status_line_styled(job_info))


class StoppingListener(ExecutionStateObserver):

    def __init__(self, server, count=1):
        self._server = server
        self.count = count

    def state_update(self, job_info: JobInfo):
        self.count -= 1
        if self.count <= 0:
            self._server.close()
