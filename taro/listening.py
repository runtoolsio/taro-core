#  Sender, Listening
from taro.job import ExecutionStateObserver
from taro.socket import SocketServer, SocketClient
from taro.util import iterates

LISTENER_FILE_EXTENSION = '.listener'


class Dispatcher(ExecutionStateObserver):

    def __init__(self):
        self._client = SocketClient(LISTENER_FILE_EXTENSION, bidirectional=False)

    @iterates
    def notify(self, job_instance):
        receiver = self._client.servers()
        while True:
            next(receiver)
            receiver.send({"event_type": "dzuvec"})

    def close(self):
        self._client.close()


class Receiver(SocketServer):

    def handle(self, req_body):
        pass
