import signal
import sys

from taro.jobs.job import ExecutionStateObserver, JobInfo
from taro.listening import StateReceiver
from taro.util import MatchingStrategy
from taroapp import printer, style
from taroapp.cmd import cliutil


def run(args):
    receiver = StateReceiver(cliutil.instance_matching_criteria(args, MatchingStrategy.PARTIAL))
    receiver.listeners.append(EventPrint(receiver))
    signal.signal(signal.SIGTERM, lambda _, __: receiver.close())
    signal.signal(signal.SIGINT, lambda _, __: receiver.close())
    receiver.start()


class EventPrint(ExecutionStateObserver):

    def __init__(self, closeable):
        self._closeable = closeable

    def state_update(self, job_info: JobInfo):
        try:
            print_state_change(job_info)
        except BrokenPipeError:
            self._closeable.close()
            cliutil.handle_broken_pipe(exit_code=1)


def print_state_change(job_info):
    printer.print_styled(*style.job_status_line_styled(job_info))
    sys.stdout.flush()
