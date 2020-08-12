import signal

from taro.listening import StateReceiver, EventPrint


def run(args):
    receiver = StateReceiver()
    receiver.listeners.append(EventPrint())
    signal.signal(signal.SIGTERM, lambda _, __: receiver.stop())
    signal.signal(signal.SIGINT, lambda _, __: receiver.stop())
    receiver.start()
