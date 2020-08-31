import signal

from taro import ExecutionStateObserver, JobInfo, ps
from taro.listening import StateReceiver


def run(args):
    receiver = StateReceiver(args.inst)
    receiver.listeners.append(EventPrint())
    signal.signal(signal.SIGTERM, lambda _, __: receiver.stop())
    signal.signal(signal.SIGINT, lambda _, __: receiver.stop())
    receiver.start()


class EventPrint(ExecutionStateObserver):

    def __init__(self, condition=lambda _: True):
        self.condition = condition

    def state_update(self, job_info: JobInfo):
        if self.condition(job_info):
            ps.print_state_change(job_info)
