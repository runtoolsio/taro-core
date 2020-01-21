#  Sender, Listening
from taro import util, dto
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
        event_body = {"event_type": "job_state_change", "event": {"job_instance": dto.job_instance(job_instance)}}

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
        job_instance = dto.to_job_instance_data(req_body['event']['job_instance'])
        for listener in self.listeners:
            listener.notify(job_instance)


class EventPrint(ExecutionStateObserver):

    def notify(self, job_instance):
        print(job_instance)


class StoppingListener(ExecutionStateObserver):

    def __init__(self, server, state):
        self._server = server
        self.state = state

    def notify(self, job_instance):
        if self.state == job_instance.state:
            self._server.stop()
