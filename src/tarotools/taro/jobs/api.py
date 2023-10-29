"""
This module provides components to expose the API of `JobInstance` objects via a local domain socket.
The main component is `APIServer`, which offers the addition or removal of `JobInstance`s using
the `add_job_instance()` and `remove_job_instance()` methods.

The domain socket with an `.api` file suffix for each server is located in the user's own subdirectory,
which is in the `/tmp` directory by default.
"""

import json
import logging
from abc import ABC, abstractmethod
from json import JSONDecodeError

from tarotools.taro.jobs.criteria import InstanceMatchCriteria
from tarotools.taro.jobs.instance import JobInstanceManager
from tarotools.taro.run import util, Flag, TerminationStatus
from tarotools.taro.socket import SocketServer

log = logging.getLogger(__name__)

API_FILE_EXTENSION = '.api'


def _create_socket_name():
    return util.unique_timestamp_hex() + API_FILE_EXTENSION


class _ApiError(Exception):

    def __init__(self, code, error):
        self.code = code
        self.error = error

    def create_response(self):
        return _resp_err(self.code, self.error)


class APIResource(ABC):

    @property
    @abstractmethod
    def path(self):
        """Path of the resource including leading '/' character"""

    def validate(self, req_body):
        """Raise :class:`__ServerError if request body is invalid"""

    @abstractmethod
    def handle(self, job_instance, req_body):
        """Handle request and optionally return response or raise :class:`__ServerError"""


class InstancesResource(APIResource):

    @property
    def path(self):
        return '/instances'

    def handle(self, job_instance, req_body):
        return {"job_instance": job_instance.create_snapshot().to_dict()}


class ReleaseWaitingResource(APIResource):

    @property
    def path(self):
        return '/instances/release/waiting'

    def validate(self, req_body):
        if 'waiting_state' not in req_body:
            raise _missing_field_error('waiting_state')

    def handle(self, job_instance, req_body):
        waiting_state = TerminationStatus[req_body['waiting_state'].upper()]
        if not waiting_state.has_flag(Flag.WAITING):
            raise _ApiError(422, f"Invalid waiting state: {waiting_state}")
        if job_instance.lifecycle.phase == waiting_state:
            job_instance.release()
            return {"release_result": 'released'}
        else:
            return {"release_result": 'not_applicable'}


class ReleasePendingResource(APIResource):

    @property
    def path(self):
        return '/instances/release/pending'

    def validate(self, req_body):
        if 'pending_group' not in req_body:
            raise _missing_field_error('pending_group')

    def handle(self, job_instance, req_body):
        pending_group = req_body['pending_group']
        if pending_group and job_instance.metadata.pending_group == pending_group:
            job_instance.release()
            return {"release_result": 'released'}
        else:
            return {"release_result": 'not_applicable'}


class StopResource(APIResource):

    @property
    def path(self):
        return '/instances/stop'

    def handle(self, job_instance, req_body):
        job_instance.stop()
        return {"stop_result": "stop_performed"}


class TailResource(APIResource):

    @property
    def path(self):
        return '/instances/tail'

    def handle(self, job_instance, req_body):
        return {"tail": job_instance.last_output}


class SignalProceedResource(APIResource):

    @property
    def path(self):
        return '/instances/_signal/dispatch'

    def handle(self, job_instance, req_body):
        waiter = job_instance.queue_waiter
        if waiter:
            executed = waiter.signal_dispatch()
        else:
            executed = False

        return {"waiter_found": waiter is not None, "executed": executed}


DEFAULT_RESOURCES = (
    InstancesResource(),
    ReleaseWaitingResource(),
    ReleasePendingResource(),
    StopResource(),
    TailResource(),
    SignalProceedResource())


class APIServer(SocketServer, JobInstanceManager):

    def __init__(self, resources=DEFAULT_RESOURCES):
        super().__init__(_create_socket_name(), allow_ping=True)
        self._resources = {resource.path: resource for resource in resources}
        self._job_instances = []

    def register_instance(self, job_instance):
        self._job_instances.append(job_instance)

    def unregister_instance(self, job_instance):
        self._job_instances.remove(job_instance)

    def handle(self, req):
        try:
            req_body = json.loads(req)
        except JSONDecodeError as e:
            log.warning(f"event=[invalid_json_request_body] length=[{e}]")
            return _resp_err(400, "invalid_req_body")

        if 'request_metadata' not in req_body:
            return _resp_err(422, "missing_field:request_metadata")

        try:
            resource = self._resolve_resource(req_body)
            resource.validate(req_body)
            job_instances = self._matching_instances(req_body)
        except _ApiError as e:
            return e.create_response()

        instance_responses = []
        for job_instance in job_instances:
            # noinspection PyBroadException
            try:
                instance_response = resource.handle(job_instance, req_body)
            except _ApiError as e:
                return e.create_response()
            except Exception:
                log.error("event=[api_handler_error]", exc_info=True)
                return _resp_err(500, 'Unexpected API handler error')
            instance_response['instance_metadata'] = job_instance.metadata.to_dict()
            instance_responses.append(instance_response)

        return _resp_ok(instance_responses)

    def _resolve_resource(self, req_body) -> APIResource:
        if 'api' not in req_body['request_metadata']:
            raise _missing_field_error('request_metadata.api')

        api = req_body['request_metadata']['api']
        resource = self._resources.get(api)
        if not resource:
            raise _ApiError(404, f"{api} API not found")

        return resource

    def _matching_instances(self, req_body):
        instance_match = req_body.get('request_metadata', {}).get('instance_match', None)
        if not instance_match:
            return self._job_instances

        try:
            matching_criteria = InstanceMatchCriteria.from_dict(instance_match)
        except ValueError:
            raise _ApiError(422, f"Invalid instance match: {instance_match}")
        return [job_instance for job_instance in self._job_instances if matching_criteria.matches(job_instance)]


def _missing_field_error(field) -> _ApiError:
    return _ApiError(422, f"Missing field {field}")


def _inst_metadata(job_instance):
    return {
        "job_id": job_instance.job_id,
        "instance_id": job_instance.instance_id
    }


def _resp_ok(instance_responses):
    return _resp(200, instance_responses)


def _resp(code: int, instance_responses):
    resp = {
        "response_metadata": {"code": code},
        "instance_responses": instance_responses
    }
    return json.dumps(resp)


def _resp_err(code: int, reason: str):
    if 400 > code >= 600:
        raise ValueError("Error code must be 4xx or 5xx")

    err_resp = {
        "response_metadata": {"code": code, "error": {"reason": reason}}
    }

    return json.dumps(err_resp)
