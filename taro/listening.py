#  Sender, Listening
from taro import util, dto, ps
from taro.job import ExecutionStateObserver, JobInfo
from taro.socket import SocketServer, SocketClient
from taro.util import iterates

STATE_LISTENER_FILE_EXTENSION = '.slistener'
OUTPUT_LISTENER_FILE_EXTENSION = '.olistener'


def _create_state_listener_socket_name():
    return util.unique_timestamp_hex() + STATE_LISTENER_FILE_EXTENSION


class StateDispatcher(ExecutionStateObserver):

    def __init__(self):
        self._client = SocketClient(STATE_LISTENER_FILE_EXTENSION, bidirectional=False)

    @iterates
    def state_update(self, job_info: JobInfo):
        event_body = {"event_type": "job_state_change", "event": {"job_info": dto.to_info_dto(job_info)}}
        self._client.communicate(event_body)

    def close(self):
        self._client.close()


class StateReceiver(SocketServer):

    def __init__(self):
        super().__init__(_create_state_listener_socket_name())
        self.listeners = []

    def handle(self, req_body):
        job_info = dto.to_job_info(req_body['event']['job_info'])
        for listener in self.listeners:
            listener.state_update(job_info)


class EventPrint(ExecutionStateObserver):

    def __init__(self, condition=lambda _: True):
        self.condition = condition

    def state_update(self, job_info: JobInfo):
        if self.condition(job_info):
            ps.print_state_change(job_info)
