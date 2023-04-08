#  Sender, Listening
import abc
import json
import logging
from abc import abstractmethod
from json import JSONDecodeError

from taro import util
from taro.jobs.events import STATE_LISTENER_FILE_EXTENSION, OUTPUT_LISTENER_FILE_EXTENSION
from taro.jobs.execution import ExecutionState
from taro.jobs.job import JobInstanceID
from taro.socket import SocketServer

log = logging.getLogger(__name__)


def _listener_socket_name(ext):
    return util.unique_timestamp_hex() + ext


def _missing_field_txt(obj, missing):
    return f"event=[invalid_event] object=[{obj}] reason=[missing field: {missing}]"


def _read_metadata(req_body_json):
    metadata = req_body_json.get('event_metadata')
    if not metadata:
        raise ValueError(_missing_field_txt('root', 'event_metadata'))

    event_type = metadata.get('event_type')
    if not event_type:
        raise ValueError(_missing_field_txt('event_metadata', 'event_type'))

    job_id = metadata.get('job_id')
    if not job_id:
        raise ValueError(_missing_field_txt('event_metadata', 'job_id'))
    instance_id = metadata.get('instance_id')
    if not instance_id:
        raise ValueError(_missing_field_txt('event_metadata', 'instance_id'))

    return event_type, JobInstanceID(job_id, instance_id)


class EventReceiver(SocketServer):

    def __init__(self, socket_name, id_match=None, event_types=()):
        super().__init__(socket_name, allow_ping=True)
        self.id_match = id_match
        self.event_types = event_types

    def handle(self, req_body):
        try:
            req_body_json = json.loads(req_body)
        except JSONDecodeError:
            log.warning(f"event=[invalid_json_event_received] length=[{len(req_body)}]")
            return

        try:
            event_type, job_instance_id = _read_metadata(req_body_json)
        except ValueError as e:
            log.warning(e)
            return

        if (self.event_types and event_type not in self.event_types) or\
                (self.id_match and not self.id_match(job_instance_id)):
            return

        self.handle_event(event_type, job_instance_id, req_body_json.get('event'))

    @abstractmethod
    def handle_event(self, event_type, job_instance_id, event):
        pass


class StateReceiver(EventReceiver):

    def __init__(self, id_match=None, states=()):
        super().__init__(_listener_socket_name(STATE_LISTENER_FILE_EXTENSION), id_match)
        self.states = states
        self.listeners = []

    def handle_event(self, _, job_instance_id, event):
        new_state = ExecutionState[event["new_state"]]

        if self.states and new_state not in self.states:
            return

        previous_state = ExecutionState[event['previous_state']]
        changed = util.parse_datetime(event["changed"])

        for listener in self.listeners:
            if isinstance(listener, ExecutionStateEventObserver):
                listener.state_update(job_instance_id, previous_state, new_state, changed)
            elif callable(listener):
                listener(job_instance_id, previous_state, new_state, changed)
            else:
                log.warning("event=[unsupported_state_observer] observer=[%s]", listener)


class ExecutionStateEventObserver(abc.ABC):
    @abc.abstractmethod
    def state_update(self, job_instance_id, previous_state, new_state, changed):
        """
        This method is called when the execution state of the job instance has changed.

        :param job_instance_id: ID of the job instance
        :param previous_state: state before
        :param new_state: new state
        :param changed: timestamp of the change
        """


class OutputReceiver(EventReceiver):

    def __init__(self, id_match=None):
        super().__init__(_listener_socket_name(OUTPUT_LISTENER_FILE_EXTENSION), id_match)
        self.listeners = []

    def handle_event(self, _, job_instance_id, event):
        output = event['output']
        is_error = event['is_error']
        for listener in self.listeners:
            listener.output_event_update(job_instance_id, output, is_error)


class OutputEventObserver(abc.ABC):

    @abc.abstractmethod
    def output_event_update(self, job_instance_id, output, is_error):
        """
        Executed when new output line is available.

        :param job_instance_id: ID of the job instance producing the output
        :param output: job instance output text
        :param is_error: True when it is an error output
        """
