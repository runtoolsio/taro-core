#  Sender, Listening
from taro import util
from taro.job import ExecutionStateObserver
from taro.socket import SocketServer, SocketClient
from taro.util import iterates

LISTENER_FILE_EXTENSION = '.listener'


def _create_socket_name():
    return util.unique_timestamp_hex() + LISTENER_FILE_EXTENSION


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

    def __init__(self):
        super().__init__(_create_socket_name())

    def handle(self, req_body):
        print(req_body)
