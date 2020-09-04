import signal
from fnmatch import fnmatch

from taro import client, JobInfo
from taro.job import JobOutputObserver
from taro.listening import OutputReceiver


def run(args):
    if args.follow:
        receiver = OutputReceiver()
        receiver.listeners.append(TailPrint())
        signal.signal(signal.SIGTERM, lambda _, __: receiver.stop())
        signal.signal(signal.SIGINT, lambda _, __: receiver.stop())
        receiver.start()
    else:
        for job_id, instance_id, tail in client.read_tail(None):
            if args.inst and not (fnmatch(job_id, args.inst) or fnmatch(instance_id, args.inst)):
                continue
            print(job_id + "@" + instance_id + ':')
            for line in tail:
                print(line)


class TailPrint(JobOutputObserver):

    def __init__(self):
        self.last_printed_job_instance = None

    def output_update(self, job_info: JobInfo, output):
        if self.last_printed_job_instance != job_info.instance_id:
            print(job_info.job_id + "@" + job_info.instance_id + ":")
        self.last_printed_job_instance = job_info.instance_id
        print(output)
