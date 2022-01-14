import signal

from taro.jobs.job import ExecutionStateObserver, JobInfo
from taro.listening import StateReceiver
from taroapp import printer, style


def run(args):
    receiver = StateReceiver(args.instance)
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
    printer.print_styled(style.job_status_line(job_info))
