import json
import logging
from abc import ABC, abstractmethod
from json import JSONDecodeError

from taro import dto, util
from taro.socket import SocketServer

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
        if 'data' not in req_body:
            raise _missing_field_error('data')
        if 'pending_group' not in req_body['data']:
            raise _missing_field_error('data.pending_group')

    def handle(self, job_instance, req_body):
        released = job_instance.release(req_body.get('data').get('pending_group'))
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
        super().__init__(_create_socket_name())
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

        try:
            resource = self._resolve_resource(req_body)
            resource.validate(req_body)
        except _ServerError as e:
            return e.create_response()

        job_instance_filter = req_body.get('job_instance')
        job_instances = [job for job in self._job_instances.copy()
                         if not job_instance_filter or job.create_info().id.matches(job_instance_filter)]
        jobs = []
        for job_instance in job_instances:
            data = resource.handle(job_instance, req_body)
            jobs.append(_job_data(job_instance, data))

        return _resp_ok(jobs)

    def _resolve_resource(self, req_body) -> APIResource:
        if 'req' not in req_body:
            raise _missing_field_error('req')
        if 'api' not in req_body['req']:
            raise _missing_field_error('req.api')

        api = req_body['req']['api']
        resource = self._resources.get(api)
        if not resource:
            raise _ServerError(404, f"{api} API not found")

        return resource


def _missing_field_error(field) -> _ServerError:
    return _ServerError(422, f"missing_field:{field}")


def _job_data(job_instance, data):
    return {
        "job_id": job_instance.job_id,
        "instance_id": job_instance.instance_id,
        "data": data
    }


def _resp_ok(jobs):
    return _resp(200, jobs)


def _resp(code: int, jobs):
    resp = {
        "resp": {"code": code},
        "jobs": jobs
    }
    return json.dumps(resp)


def _resp_err(code: int, error: str):
    if 400 > code >= 600:
        raise ValueError("Error code must be 4xx or 5xx")

    err_resp = {
        "resp": {"code": code},
        "error": error
    }

    return json.dumps(err_resp)
