import sys

import taro.client
from taro import JobInfo
from taro.jobs.job import JobOutputObserver
from taro.listening import OutputReceiver
from taro.theme import Theme
from taro.util import MatchingStrategy
from taroapp import printer, style, cliutil

HIGHLIGHT_TOKEN = (Theme.separator, ' ---> ')


def run(args):
    instance_match = cliutil.instance_matching_criteria(args, MatchingStrategy.PARTIAL)
    if args.follow:
        receiver = OutputReceiver(instance_match)
        receiver.listeners.append(TailPrint(receiver))
        receiver.start()
        cliutil.exit_on_signal(cleanups=[receiver.close_and_wait])
        receiver.wait()  # Prevents 'exception ignored in: <module 'threading' from ...>` error message
    else:
        for tail_resp in taro.client.read_tail(instance_match).responses:
            job_id, instance_id = tail_resp.id
            printer.print_styled(HIGHLIGHT_TOKEN, *style.job_instance_id_styled(job_id, instance_id))
            for line in tail_resp.tail:
                print(line)
            sys.stdout.flush()


class TailPrint(JobOutputObserver):

    def __init__(self, receiver):
        self._receiver = receiver
        self.last_printed_job_instance = None

    def output_update(self, job_info: JobInfo, output):
        # TODO It seems that this needs locking
        try:
            if self.last_printed_job_instance != job_info.instance_id:
                printer.print_styled(HIGHLIGHT_TOKEN, *style.job_instance_styled(job_info))
            self.last_printed_job_instance = job_info.instance_id
            print(output, flush=True)
        except BrokenPipeError:
            self._receiver.close_and_wait()
            cliutil.handle_broken_pipe(exit_code=1)
