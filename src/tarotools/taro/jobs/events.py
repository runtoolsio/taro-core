"""
This module contains event producers as part of the Job Instance Event framework. The event consumers are expected
to create a domain socket with a corresponding file suffix located in the user's own subdirectory,
which is in the `/tmp` directory by default. The sockets are used by the producers to send the events.
In this design, the communication is unidirectional, with servers acting as consumers and clients as producers.
"""

import abc
import json
import logging

from tarotools.taro import util
from tarotools.taro.jobs.inst import ExecutionStateObserver, JobInst, JobOutputObserver
from tarotools.taro.socket import SocketClient, PayloadTooLarge
from tarotools.taro.util import format_dt_iso

STATE_LISTENER_FILE_EXTENSION = '.slistener'
OUTPUT_LISTENER_FILE_EXTENSION = '.olistener'

log = logging.getLogger(__name__)


class EventDispatcher(abc.ABC):
    """
    This serves as a parent class for event producers. The subclasses (children) are expected to provide a specific
    socket client and utilize the `_send_event` method to dispatch events to the respective consumers.
    """

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
    """
    This producer emits an event when the state of a job instance changes. This dispatcher should be registered to
    the job instance as an `ExecutionStateObserver`.
    """

    def __init__(self):
        super(StateDispatcher, self).__init__(SocketClient(STATE_LISTENER_FILE_EXTENSION, bidirectional=False))

    def state_update(self, job_inst: JobInst, previous_state, new_state, changed):
        event = {
            "new_state": new_state.name,
            "previous_state": previous_state.name,
            "changed": format_dt_iso(changed),
        }
        self._send_event("execution_state_change", job_inst.metadata, event)


class OutputDispatcher(EventDispatcher, JobOutputObserver):
    """
    This producer emits an event when a job instance generates a new output. This dispatcher should be registered to
    the job instance as an `JobOutputObserver`.
    """

    def __init__(self):
        super(OutputDispatcher, self).__init__(SocketClient(OUTPUT_LISTENER_FILE_EXTENSION, bidirectional=False))

    def job_output_update(self, job_info: JobInst, output, is_error):
        event = {
            "output": util.truncate(output, 10000, truncated_suffix=".. (truncated)"),
            "is_error": is_error,
        }
        self._send_event("new_output", job_info.metadata, event)
