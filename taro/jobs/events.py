import abc
import json
import logging

from taro import dto, util
from taro.jobs.job import ExecutionStateObserver, JobInfo, JobOutputObserver
from taro.socket import SocketClient, PayloadTooLarge

STATE_LISTENER_FILE_EXTENSION = '.slistener'
OUTPUT_LISTENER_FILE_EXTENSION = '.olistener'

log = logging.getLogger(__name__)


class EventDispatcher(abc.ABC):

    @abc.abstractmethod
    def __init__(self, client):
        self._client = client

    def _send_event(self, type_, job_info, event):
        event_body = {"event_metadata": {"event_type": type_, "job_info": dto.to_info_dto(job_info)}, "event": event}
        try:
            self._client.communicate(json.dumps(event_body))
        except PayloadTooLarge:
            log.warning("event=[event_dispatch_failed] reason=[payload_too_large] note=[Please report this issue!]")

    def close(self):
        self._client.close()


class StateDispatcher(EventDispatcher, ExecutionStateObserver):

    def __init__(self):
        super(StateDispatcher, self).__init__(SocketClient(STATE_LISTENER_FILE_EXTENSION, bidirectional=False))

    def state_update(self, job_info: JobInfo):
        event = {"new_state": job_info.state.name}
        self._send_event("execution_state_change", job_info, event)


class OutputDispatcher(EventDispatcher, JobOutputObserver):

    def __init__(self):
        super(OutputDispatcher, self).__init__(SocketClient(OUTPUT_LISTENER_FILE_EXTENSION, bidirectional=False))

    def output_update(self, job_info: JobInfo, output, is_error):
        event = {
            "output": util.truncate(output, 10000, truncated_suffix=".. (truncated)"),
            "is_error": is_error,
        }
        self._send_event("new_output", job_info, event)
