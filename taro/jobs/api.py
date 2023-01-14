import json
import logging
from abc import ABC, abstractmethod
from json import JSONDecodeError

from taro import dto, util
from taro.socket import SocketServer
from taro.util import MatchingStrategy

log = logging.getLogger(__name__)

API_FILE_EXTENSION = '.api'


def _create_socket_name():
    return util.unique_timestamp_hex() + API_FILE_EXTENSION


class _ServerError(Exception):

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


class JobsResource(APIResource):

    @property
    def path(self):
        return '/jobs'

    def handle(self, job_instance, req_body):
        return {"job_info": dto.to_info_dto(job_instance.create_info())}


class ReleaseResource(APIResource):

    @property
    def path(self):
        return '/jobs/release'

    def validate(self, req_body):
        if 'pending_group' not in req_body:
            raise _missing_field_error('pending_group')

    def handle(self, job_instance, req_body):
        released = job_instance.release(req_body['pending_group'])
        return {"released": released}


class StopResource(APIResource):

    @property
    def path(self):
        return '/jobs/stop'

    def handle(self, job_instance, req_body):
        job_instance.stop()
        return {"result": "stop_performed"}


class TailResource(APIResource):

    @property
    def path(self):
        return '/jobs/tail'

    def handle(self, job_instance, req_body):
        return {"tail": job_instance.last_output}


DEFAULT_RESOURCES = JobsResource(), ReleaseResource(), StopResource(), TailResource()


class Server(SocketServer):

    def __init__(self, resources=DEFAULT_RESOURCES):
        super().__init__(_create_socket_name(), allow_ping=True)
        self._resources = {resource.path: resource for resource in resources}
        self._job_instances = []

    def add_job_instance(self, job_instance):
        self._job_instances.append(job_instance)

    def remove_job_instance(self, job_instance):
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
        except _ServerError as e:
            return e.create_response()

        instance_responses = []
        for job_instance in job_instances:
            instance_response = resource.handle(job_instance, req_body)
            instance_response['instance_metadata'] = _inst_metadata(job_instance)
            instance_responses.append(instance_response)

        return _resp_ok(instance_responses)

    def _resolve_resource(self, req_body) -> APIResource:
        if 'api' not in req_body['request_metadata']:
            raise _missing_field_error('request_metadata.api')

        api = req_body['request_metadata']['api']
        resource = self._resources.get(api)
        if not resource:
            raise _ServerError(404, f"{api} API not found")

        return resource

    def _matching_instances(self, req_body):
        match = req_body.get('request_metadata', {}).get('instance_match', {})
        match_ids = match.get('ids', None)
        if not match_ids:
            return self._job_instances

        if match_strategy_val := match.get('id_match_strategy'):
            try:
                match_strategy = MatchingStrategy[match_strategy_val.upper()]
            except KeyError as e:
                raise _ServerError(422, f"Invalid id_match_strategy value: {match_strategy_val}")
        else:
            match_strategy = MatchingStrategy.EXACT
        return [job_instance for job_instance in self._job_instances
                if any(1 for match_id in match_ids if job_instance.id.matches(match_id, match_strategy))]


def _missing_field_error(field) -> _ServerError:
    return _ServerError(422, f"Missing field {field}")


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
        "instances": instance_responses
    }
    return json.dumps(resp)


def _resp_err(code: int, reason: str):
    if 400 > code >= 600:
        raise ValueError("Error code must be 4xx or 5xx")

    err_resp = {
        "response_metadata": {"code": code, "error": {"reason": reason}}
    }

    return json.dumps(err_resp)
