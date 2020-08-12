import signal
import sys

from taro import ExecutionStateObserver, JobInfo
from taro.listening import StateReceiver, EventPrint


def run(args):
    def condition(job_info): return not args.states or job_info.state.name in args.states

    receiver = StateReceiver()
    receiver.listeners.append(EventPrint(condition))
    receiver.listeners.append(StoppingListener(receiver, condition, args.count))
    signal.signal(signal.SIGTERM, lambda _, __: _stop_server_and_exit(receiver, signal.SIGTERM))
    signal.signal(signal.SIGINT, lambda _, __: _stop_server_and_exit(receiver, signal.SIGINT))
    receiver.start()


def _stop_server_and_exit(server, signal_number: int):
    server.stop()
    sys.exit(128 + signal_number)


class StoppingListener(ExecutionStateObserver):

    def __init__(self, server, condition=lambda _: True, count=1):
        self._server = server
        self.condition = condition
        self.count = count

    def state_update(self, job_info: JobInfo):
        if self.condition(job_info):
            self.count -= 1
            if self.count <= 0:
                self._server.stop()
