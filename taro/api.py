import logging
from typing import List, Tuple

from taro import dto
from taro.job import JobInfo
from taro.socket import SocketServer, SocketClient
from taro.util import iterates

log = logging.getLogger(__name__)

API_FILE_EXTENSION = '.api'


def _create_socket_name(job_info):
    return job_info.instance_id + API_FILE_EXTENSION


def _resp(code: int, instance_id: str, data):
    return {"resp": {"code": code}, "instance": {"id": instance_id}, "data": data}


def _resp_err(code: int, error: str):
    if 400 > code >= 600:
        raise ValueError("Error code must be 4xx or 5xx")
    return {"resp": {"code": code}, "error": error}


class Server(SocketServer):

    def __init__(self, job_control, latch_release):
        super().__init__(_create_socket_name(job_control))
        self._job_control = job_control
        self._latch_release = latch_release

    def handle(self, req_body):
        if 'req' not in req_body:
            return _resp_err(422, "missing_req")
        if 'api' not in req_body['req']:
            return _resp_err(422, "missing_req_api")

        inst_id = self._job_control.instance_id

        if req_body['req']['api'] == '/job':
            info_dto = dto.to_info_dto(self._job_control.create_info())
            return _resp(200, inst_id, {"job_info": info_dto})

        if req_body['req']['api'] == '/release':
            if 'data' not in req_body:
                return _resp_err(422, "missing_data")
            if 'pending' not in req_body['data']:
                return _resp_err(422, "missing_data_field:pending")

            if self._latch_release:
                released = self._latch_release.release(req_body.get('data').get('pending'))
            else:
                released = False
            return _resp(200, inst_id, {"released": released})

        if req_body['req']['api'] == '/stop':
            self._job_control.stop()
            return _resp(200, inst_id, {"result": "stop_performed"})

        if req_body['req']['api'] == '/interrupt':
            self._job_control.interrupt()
            return _resp(200, inst_id, {"result": "interrupt_performed"})

        return _resp_err(404, "req_api_not_found")


def _create_job_info(info_resp):
    return dto.to_job_info(info_resp.response['data']['job_info'])


class Client(SocketClient):

    def __init__(self):
        super().__init__(API_FILE_EXTENSION, bidirectional=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def read_jobs_info(self) -> List[JobInfo]:
        responses = self.communicate({'req': {'api': '/job'}})
        return [_create_job_info(inst_resp) for inst_resp in responses]

    @iterates
    def release_jobs(self, pending):
        server = self.servers()
        while True:
            next(server)
            resp = server.send({'req': {'api': '/release'}, "data": {"pending": pending}}).response
            if resp['data']['released']:
                print(resp)  # TODO Do not print, but returned released (use communicate)

    def stop_jobs(self, instances, interrupt: bool) -> List[Tuple[str, str]]:
        """

        :param instances:
        :param interrupt:
        :return: list of tuple[instance-id, stop-result]
        """
        if not instances:
            raise ValueError('Instances to be stopped cannot be empty')

        inst_responses = self.communicate({'req': {'api': '/interrupt' if interrupt else '/stop'}}, instances)
        return [(inst_resp.instance, inst_resp.response['data']['result']) for inst_resp in inst_responses]
