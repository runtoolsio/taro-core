import signal

from taro.job import ExecutionStateObserver, JobInfo
from taro.listening import StateReceiver


def run(args):
    receiver = StateReceiver(args.inst)
    receiver.listeners.append(EventPrint())
    signal.signal(signal.SIGTERM, lambda _, __: receiver.close())
    signal.signal(signal.SIGINT, lambda _, __: receiver.close())
    receiver.start()


class EventPrint(ExecutionStateObserver):

    def __init__(self, condition=lambda _: True):
        self.condition = condition

    def state_update(self, job_info: JobInfo):
        if self.condition(job_info):
            print_state_change(job_info)


def print_state_change(job_info):
    print(f"{job_info.job_id}@{job_info.instance_id} -> {job_info.state.name}")
