import signal
from fnmatch import fnmatch

import taro.client
from taro import JobInfo
from taro.jobs.job import JobOutputObserver
from taro.listening import OutputReceiver


def run(args):
    if args.follow:
        receiver = OutputReceiver(args.inst)
        receiver.listeners.append(TailPrint())
        signal.signal(signal.SIGTERM, lambda _, __: receiver.close())
        signal.signal(signal.SIGINT, lambda _, __: receiver.close())
        receiver.start()
    else:
        for job_id, instance_id, tail in taro.client.read_tail(None):
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
