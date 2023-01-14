import sys

from taro.jobs.job import ExecutionStateObserver, JobInfo
from taro.listening import StateReceiver
from taro.util import MatchingStrategy
from taroapp import printer, style, cliutil


def run(args):
    receiver = StateReceiver(cliutil.instance_matching_criteria(args, MatchingStrategy.PARTIAL))
    receiver.listeners.append(EventPrint(receiver))
    receiver.start()
    cliutil.exit_on_signal(cleanups=[receiver.close_and_wait])
    receiver.wait()  # Prevents 'exception ignored in: <module 'threading' from ...>` error message


class EventPrint(ExecutionStateObserver):

    def __init__(self, receiver):
        self._receiver = receiver

    def state_update(self, job_info: JobInfo):
        try:
            print_state_change(job_info)
        except BrokenPipeError:
            self._receiver.close_and_wait()
            cliutil.handle_broken_pipe(exit_code=1)


def print_state_change(job_info):
    printer.print_styled(*style.job_status_line_styled(job_info))
    sys.stdout.flush()
