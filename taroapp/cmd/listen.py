import sys

from taro.listening import StateReceiver, ExecutionStateEventObserver
from taro.util import MatchingStrategy
from taroapp import argsutil
from taroapp import printer, style, cliutil


def run(args):
    receiver = StateReceiver(argsutil.id_matching_criteria(args, MatchingStrategy.PARTIAL))
    receiver.listeners.append(EventPrint(receiver))
    receiver.start()
    cliutil.exit_on_signal(cleanups=[receiver.close_and_wait])
    receiver.wait()  # Prevents 'exception ignored in: <module 'threading' from ...>` error message


class EventPrint(ExecutionStateEventObserver):

    def __init__(self, receiver):
        self._receiver = receiver

    def state_update(self, job_instance_id, previous_state, new_state, changed):
        try:
            printer.print_styled(*style.job_instance_id_status_line_styled(job_instance_id, new_state, changed))
            sys.stdout.flush()
        except BrokenPipeError:
            self._receiver.close_and_wait()
            cliutil.handle_broken_pipe(exit_code=1)
