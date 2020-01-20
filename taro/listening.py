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
        event_body = {"event_type": "job_state_change", "event": {"job_instance": {
            "job_id": job_instance.job_id, "instance_id": job_instance.instance_id, "state": job_instance.state.name,
            "exec_error": job_instance.exec_error.message if job_instance.exec_error else None
        }}}
        receiver = self._client.servers()
        while True:
            next(receiver)
            receiver.send(event_body)

    def close(self):
        self._client.close()


class Receiver(SocketServer):

    def __init__(self):
        super().__init__(_create_socket_name())
        self.listeners = []

    def handle(self, req_body):
        print(req_body)
