#  Sender, Listening
import json
import logging
from abc import abstractmethod
from json import JSONDecodeError

from tarotools.job.events import TRANSITION_LISTENER_FILE_EXTENSION, OUTPUT_LISTENER_FILE_EXTENSION

from tarotools.taro import util, paths
from tarotools.taro.job import JobInstanceMetadata, JobRun, InstanceTransitionObserver, InstanceOutputObserver
from tarotools.taro.run import PhaseRun, PhaseMetadata
from tarotools.taro.util.observer import ObservableNotification
from tarotools.taro.util.socket import SocketServer

log = logging.getLogger(__name__)


def _listener_socket_name(ext):
    return util.unique_timestamp_hex() + ext


def _missing_field_txt(obj, missing):
    return f"event=[invalid_event] object=[{obj}] reason=[missing field: {missing}]"


def _read_metadata(req_body_json):
    event_metadata = req_body_json.get('event_metadata')
    if not event_metadata:
        raise ValueError(_missing_field_txt('root', 'event_metadata'))

    event_type = event_metadata.get('event_type')
    if not event_type:
        raise ValueError(_missing_field_txt('event_metadata', 'event_type'))

    instance_metadata = req_body_json.get('instance_metadata')
    if not instance_metadata:
        raise ValueError(_missing_field_txt('root', 'instance_metadata'))

    return event_type, JobInstanceMetadata.deserialize(instance_metadata)


class EventReceiver(SocketServer):

    def __init__(self, socket_name, id_match=None, event_types=()):
        super().__init__(lambda: paths.socket_path(socket_name, create=True), allow_ping=True)
        self.id_match = id_match
        self.event_types = event_types

    def handle(self, req_body):
        try:
            req_body_json = json.loads(req_body)
        except JSONDecodeError:
            log.warning(f"event=[invalid_json_event_received] length=[{len(req_body)}]")
            return

        try:
            event_type, instance_meta = _read_metadata(req_body_json)
        except ValueError as e:
            log.warning(e)
            return

        if (self.event_types and event_type not in self.event_types) or \
                (self.id_match and not self.id_match(instance_meta.id)):
            return

        self.handle_event(event_type, instance_meta, req_body_json.get('event'))

    @abstractmethod
    def handle_event(self, event_type, instance_meta, event):
        pass


class InstanceTransitionReceiver(EventReceiver):

    def __init__(self, id_match=None, phases=()):
        super().__init__(_listener_socket_name(TRANSITION_LISTENER_FILE_EXTENSION), id_match)
        self.phases = phases
        self._notification = ObservableNotification[InstanceTransitionObserver]()

    def handle_event(self, _, instance_meta, event):
        new_phase = PhaseRun.deserialize(event["new_phase"])

        if self.phases and new_phase.phase_name not in self.phases:
            return

        job_run = JobRun.deserialize(event['job_run'])
        previous_phase = PhaseRun.deserialize(event['previous_phase'])
        ordinal = event['ordinal']

        self._notification.observer_proxy.new_instance_phase(job_run, previous_phase, new_phase, ordinal)

    def add_observer_transition(self, observer):
        self._notification.add_observer(observer)

    def remove_observer_transition(self, observer):
        self._notification.remove_observer(observer)


class InstanceOutputReceiver(EventReceiver):

    def __init__(self, id_match=None):
        super().__init__(_listener_socket_name(OUTPUT_LISTENER_FILE_EXTENSION), id_match)
        self._notification = ObservableNotification[InstanceOutputObserver]()

    def handle_event(self, _, instance_meta, event):
        phase = PhaseMetadata.deserialize(event['phase'])
        output = event['output']
        is_error = event['is_error']
        self._notification.observer_proxy.new_instance_output(instance_meta, phase, output, is_error)

    def add_observer_output(self, observer):
        self._notification.add_observer(observer)

    def remove_observer_output(self, observer):
        self._notification.remove_observer(observer)
