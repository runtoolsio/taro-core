import signal
import sys

from taro.listening import Receiver, EventPrint, StoppingListener


def run(args):
    def condition(job_info): return not args.states or job_info.state.name in args.states

    receiver = Receiver()
    receiver.listeners.append(EventPrint(condition))
    receiver.listeners.append(StoppingListener(receiver, condition, args.count))
    signal.signal(signal.SIGTERM, lambda _, __: _stop_server_and_exit(receiver, signal.SIGTERM))
    signal.signal(signal.SIGINT, lambda _, __: _stop_server_and_exit(receiver, signal.SIGINT))
    receiver.start()


def _stop_server_and_exit(server, signal_number: int):
    server.stop()
    sys.exit(128 + signal_number)
