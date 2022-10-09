import logging

from taro import ExecutionStateObserver, JobInfo, dto
from taro.jobs.job import JobOutputObserver
from taro.socket import SocketClient, PayloadTooLarge

STATE_LISTENER_FILE_EXTENSION = '.slistener'
OUTPUT_LISTENER_FILE_EXTENSION = '.olistener'

log = logging.getLogger(__name__)


class StateDispatcher(ExecutionStateObserver):

    def __init__(self):
        self._client = SocketClient(STATE_LISTENER_FILE_EXTENSION, bidirectional=False)

    def state_update(self, job_info: JobInfo):
        event_body = {"event_type": "execution_state_change", "event": {"job_info": dto.to_info_dto(job_info)}}
        try:
            self._client.communicate(event_body)
        except PayloadTooLarge:
            log.warning("event=[state_dispatch_failed] reason=[payload_too_large] note=[Please report this issue!]")

    def close(self):
        self._client.close()


class OutputDispatcher(JobOutputObserver):

    def __init__(self):
        self._client = SocketClient(OUTPUT_LISTENER_FILE_EXTENSION, bidirectional=False)

    def output_update(self, job_info: JobInfo, output):
        event_body = {"event_type": "new_output", "event": {"job_info": dto.to_info_dto(job_info), "output": output}}
        try:
            self._client.communicate(event_body)
        except PayloadTooLarge:
            log.warning("event=[output_dispatch_failed] reason=[payload_too_large] note=[Please report this issue!]")

    def close(self):
        self._client.close()
