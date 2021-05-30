#  Sender, Listening
import logging

from taro import util, dto
from taro.jobs.events import STATE_LISTENER_FILE_EXTENSION, OUTPUT_LISTENER_FILE_EXTENSION
from taro.jobs.job import ExecutionStateObserver
from taro.socket import SocketServer

log = logging.getLogger(__name__)


def _listener_socket_name(ext):
    return util.unique_timestamp_hex() + ext


class StateReceiver(SocketServer):

    def __init__(self, instance="", states=()):
        super().__init__(_listener_socket_name(STATE_LISTENER_FILE_EXTENSION))
        self.instance = instance
        self.states = states
        self.listeners = []

    def handle(self, req_body):
        job_info = dto.to_job_info(req_body['event']['job_info'])
        if self.instance and not job_info.matches(self.instance):
            return
        if self.states and job_info.state not in self.states:
            return
        for listener in self.listeners:
            if isinstance(listener, ExecutionStateObserver):
                listener.state_update(job_info)
            elif callable(listener):
                listener(job_info)
            else:
                log.warning("event=[unsupported_state_observer] observer=[%s]", listener)


class OutputReceiver(SocketServer):

    def __init__(self, instance=""):
        super().__init__(_listener_socket_name(OUTPUT_LISTENER_FILE_EXTENSION))
        self.instance = instance
        self.listeners = []

    def handle(self, req_body):
        job_info = dto.to_job_info(req_body['event']['job_info'])
        if self.instance and not job_info.matches(self.instance):
            return
        output = req_body['event']['output']
        for listener in self.listeners:
            listener.output_update(job_info, output)
