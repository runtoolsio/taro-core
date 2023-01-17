import sys

import taro.client
from taro.jobs.job import InstanceMatchingCriteria
from taro.listening import OutputReceiver, OutputEventObserver
from taro.theme import Theme
from taro.util import MatchingStrategy
from taroapp import printer, style, cliutil

HIGHLIGHT_TOKEN = (Theme.separator, ' ---> ')


def run(args):
    id_match = cliutil.id_matching_criteria(args, MatchingStrategy.PARTIAL)
    if args.follow:
        receiver = OutputReceiver(id_match)
        receiver.listeners.append(TailPrint(receiver))
        receiver.start()
        cliutil.exit_on_signal(cleanups=[receiver.close_and_wait])
        receiver.wait()  # Prevents 'exception ignored in: <module 'threading' from ...>` error message
    else:
        for tail_resp in taro.client.read_tail(InstanceMatchingCriteria(id_match)).responses:
            printer.print_styled(HIGHLIGHT_TOKEN, *style.job_instance_id_styled(tail_resp.id))
            for line, is_error in tail_resp.tail:
                print(line, file=sys.stderr if is_error else sys.stdout)
            sys.stdout.flush()


class TailPrint(OutputEventObserver):

    def __init__(self, receiver):
        self._receiver = receiver
        self.last_printed_job_instance = None

    def output_update(self, job_instance_id, output, is_error):
        # TODO It seems that this needs locking
        try:
            if self.last_printed_job_instance != job_instance_id:
                printer.print_styled(HIGHLIGHT_TOKEN, *style.job_instance_id_styled(job_instance_id))
            self.last_printed_job_instance = job_instance_id
            print(output, flush=True, file=sys.stderr if is_error else sys.stdout)
        except BrokenPipeError:
            self._receiver.close_and_wait()
            cliutil.handle_broken_pipe(exit_code=1)
