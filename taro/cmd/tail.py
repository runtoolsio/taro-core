import signal

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
        for instance, tail in client.read_tail(None):
            print(instance + ':')
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
