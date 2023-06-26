import abc
import json
import logging

from taro import util
from taro.jobs.inst import ExecutionStateObserver, JobInst, JobOutputObserver
from taro.socket import SocketClient, PayloadTooLarge
from taro.util import format_dt_iso

STATE_LISTENER_FILE_EXTENSION = '.slistener'
OUTPUT_LISTENER_FILE_EXTENSION = '.olistener'

log = logging.getLogger(__name__)


class EventDispatcher(abc.ABC):

    @abc.abstractmethod
    def __init__(self, client):
        self._client = client

    def _send_event(self, event_type, instance_meta, event):
        event_body = {
            "event_metadata": {
                "event_type": event_type
            },
            "instance_metadata": instance_meta.to_dict(),
            "event": event
        }
        try:
            self._client.communicate(json.dumps(event_body))
        except PayloadTooLarge:
            log.warning("event=[event_dispatch_failed] reason=[payload_too_large] note=[Please report this issue!]")

    def close(self):
        self._client.close()


class StateDispatcher(EventDispatcher, ExecutionStateObserver):

    def __init__(self):
        super(StateDispatcher, self).__init__(SocketClient(STATE_LISTENER_FILE_EXTENSION, bidirectional=False))

    def state_update(self, job_info: JobInst):
        event = {
            "new_state": job_info.state.name,
            "previous_state": None,  # TODO previous_state
            "changed": format_dt_iso(job_info.lifecycle.last_changed_at),
        }
        self._send_event("execution_state_change", job_info.metadata, event)


class OutputDispatcher(EventDispatcher, JobOutputObserver):

    def __init__(self):
        super(OutputDispatcher, self).__init__(SocketClient(OUTPUT_LISTENER_FILE_EXTENSION, bidirectional=False))

    def job_output_update(self, job_info: JobInst, output, is_error):
        event = {
            "output": util.truncate(output, 10000, truncated_suffix=".. (truncated)"),
            "is_error": is_error,
        }
        self._send_event("new_output", job_info.metadata, event)
