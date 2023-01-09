#  Sender, Listening
import json
import logging
from abc import abstractmethod
from json import JSONDecodeError

from taro import util, dto
from taro.jobs.events import STATE_LISTENER_FILE_EXTENSION, OUTPUT_LISTENER_FILE_EXTENSION
from taro.jobs.job import ExecutionStateObserver
from taro.socket import SocketServer

log = logging.getLogger(__name__)


def _listener_socket_name(ext):
    return util.unique_timestamp_hex() + ext


class EventReceiver(SocketServer):

    def __init__(self, socket_name, instance_match=None):
        super().__init__(socket_name)
        self.instance_match = instance_match
        self.listeners = []

    def handle(self, req_body):
        try:
            req_body_json = json.loads(req_body)
        except JSONDecodeError:
            log.warning(f"event=[invalid_json_event_received] length=[{len(req_body)}]")
            return

        job_info = dto.to_job_info(req_body_json['event']['job_info'])
        if self.instance_match and not job_info.matches(self.instance_match):
            return

        self.handle_event(job_info, req_body_json['event'])

    @abstractmethod
    def handle_event(self, job_info, event):
        pass


class StateReceiver(EventReceiver):

    def __init__(self, instance_match=None, states=()):
        super().__init__(_listener_socket_name(STATE_LISTENER_FILE_EXTENSION), instance_match)
        self.states = states

    def handle_event(self, job_info, _):
        if self.states and job_info.state not in self.states:
            return

        for listener in self.listeners:
            if isinstance(listener, ExecutionStateObserver):
                listener.state_update(job_info)
            elif callable(listener):
                listener(job_info)
            else:
                log.warning("event=[unsupported_state_observer] observer=[%s]", listener)


class OutputReceiver(EventReceiver):

    def __init__(self, instance_match=None):
        super().__init__(_listener_socket_name(OUTPUT_LISTENER_FILE_EXTENSION), instance_match)

    def handle_event(self, job_info, event):
        output = event['output']
        for listener in self.listeners:
            listener.output_update(job_info, output)
