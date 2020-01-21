#  Sender, Listening
from taro import util
from taro.execution import ExecutionState, ExecutionError
from taro.job import ExecutionStateObserver, JobInstanceData
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
        if job_instance.exec_error:
            error_part = {"message": job_instance.exec_error.message, "state": job_instance.exec_error.exec_state.name}
        else:
            error_part = None
        event_body = {"event_type": "job_state_change", "event": {"job_instance": {
            "job_id": job_instance.job_id, "instance_id": job_instance.instance_id, "state": job_instance.state.name,
            "exec_error": error_part
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
        ji = req_body['event']['job_instance']
        if ji['exec_error']:
            exec_error = ExecutionError(ji['exec_error']['message'], ExecutionState[ji['exec_error']['state']])
        else:
            exec_error = None
        job_instance = JobInstanceData(ji['job_id'], ji['instance_id'], ExecutionState[ji['state']], exec_error)

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
