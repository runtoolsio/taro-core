import signal
from fnmatch import fnmatch

import taro.client
from taro import JobInfo
from taro.jobs.job import JobOutputObserver
from taro.listening import OutputReceiver
from taro.theme import Theme
from taroapp import printer, style

HIGHLIGHT_TOKEN = (Theme.separator, ' ---> ')


def run(args):
    if args.follow:
        receiver = OutputReceiver(args.instance)
        receiver.listeners.append(TailPrint())
        signal.signal(signal.SIGTERM, lambda _, __: receiver.close())
        signal.signal(signal.SIGINT, lambda _, __: receiver.close())
        receiver.start()
    else:
        for (job_id, instance_id), tail in taro.client.read_tail(None)[0]:
            if args.instance and not (fnmatch(job_id, args.instance) or fnmatch(instance_id, args.instance)):
                continue
            printer.print_styled(HIGHLIGHT_TOKEN, *style.job_instance_id_styled(job_id, instance_id))
            for line in tail:
                print(line)


class TailPrint(JobOutputObserver):

    def __init__(self):
        self.last_printed_job_instance = None

    def output_update(self, job_info: JobInfo, output):
        # TODO It seems that this needs locking
        if self.last_printed_job_instance != job_info.instance_id:
            printer.print_styled(HIGHLIGHT_TOKEN, *style.job_instance_styled(job_info))
        self.last_printed_job_instance = job_info.instance_id
        print(output)
