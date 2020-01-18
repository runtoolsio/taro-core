#  Sender, Listening
from taro.job import ExecutionStateObserver
from taro.socket import SocketServer

LISTENER_FILE_EXTENSION = '.listener'


class Dispatcher(ExecutionStateObserver):

    def __init__(self):
        pass

    def notify(self, job_instance):
        pass


class Receiver(SocketServer):

    def handle(self, req_body):
        pass
