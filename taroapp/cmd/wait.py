"""
TODO: Create option where the command will terminates if the specified state is found in the previous or current state
      of an existing instance.
"""
from taro.listening import StateReceiver, ExecutionStateEventObserver
from taro.util import MatchingStrategy, DateTimeFormat
from taroapp import argsutil
from taroapp import printer, style, cliutil


def run(args):
    instance_match = argsutil.id_matching_criteria(args, MatchingStrategy.PARTIAL)
    receiver = StateReceiver(instance_match, args.states)
    receiver.listeners.append(EventHandler(receiver, args.count, args.timestamp.value))
    receiver.start()
    cliutil.exit_on_signal(cleanups=[receiver.close_and_wait])
    receiver.wait()  # Prevents 'exception ignored in: <module 'threading' from ...>` error message, remove when fixed


class EventHandler(ExecutionStateEventObserver):

    def __init__(self, receiver, count=1, ts_format=DateTimeFormat.DATE_TIME_MS_LOCAL_ZONE):
        self._receiver = receiver
        self.count = count
        self.ts_format = ts_format

    def state_update(self, job_instance_id, previous_state, new_state, changed):
        try:
            printer.print_styled(*style.job_instance_id_status_line_styled(
                job_instance_id, new_state, changed, ts_prefix_format=self.ts_format))
        except BrokenPipeError:
            self._receiver.close_and_wait()
            cliutil.handle_broken_pipe(exit_code=1)

        self.count -= 1
        if self.count <= 0:
            self._receiver.close()
