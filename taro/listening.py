#  Sender, Listening
from taro import util, dto
from taro.job import ExecutionStateObserver, JobInfo, JobOutputObserver
from taro.socket import SocketServer, SocketClient

STATE_LISTENER_FILE_EXTENSION = '.slistener'
OUTPUT_LISTENER_FILE_EXTENSION = '.olistener'


def _listener_socket_name(ext):
    return util.unique_timestamp_hex() + ext


class StateDispatcher(ExecutionStateObserver):

    def __init__(self):
        self._client = SocketClient(STATE_LISTENER_FILE_EXTENSION, bidirectional=False)

    def state_update(self, job_info: JobInfo):
        event_body = {"event_type": "execution_state_change", "event": {"job_info": dto.to_info_dto(job_info)}}
        self._client.communicate(event_body)

    def close(self):
        self._client.close()


class StateReceiver(SocketServer):

    def __init__(self, instance=""):
        super().__init__(_listener_socket_name(STATE_LISTENER_FILE_EXTENSION))
        self.instance = instance
        self.listeners = []

    def handle(self, req_body):
        job_info = dto.to_job_info(req_body['event']['job_info'])
        if self.instance and not job_info.matches(self.instance):
            return
        for listener in self.listeners:
            listener.state_update(job_info)


class OutputDispatcher(JobOutputObserver):

    def __init__(self):
        self._client = SocketClient(OUTPUT_LISTENER_FILE_EXTENSION, bidirectional=False)

    def output_update(self, job_info: JobInfo, output):
        event_body = {"event_type": "new_output", "event": {"job_info": dto.to_info_dto(job_info), "output": output}}
        self._client.communicate(event_body)

    def close(self):
        self._client.close()


class OutputReceiver(SocketServer):

    def __init__(self):
        super().__init__(_listener_socket_name(OUTPUT_LISTENER_FILE_EXTENSION))
        self.listeners = []

    def handle(self, req_body):
        job_info = dto.to_job_info(req_body['event']['job_info'])
        output = req_body['event']['output']
        for listener in self.listeners:
            listener.output_update(job_info, output)
