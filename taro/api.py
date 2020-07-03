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


class Server(SocketServer):

    def __init__(self, job_control, latch_release):
        super().__init__(_create_socket_name(job_control))
        self._job_control = job_control
        self._latch_release = latch_release

    def handle(self, req_body):
        if 'req' not in req_body:
            return {"resp": {"error": "missing_req"}}
        if 'api' not in req_body['req']:
            return {"resp": {"error": "missing_req_api"}}

        info_dto = dto.to_info_dto(self._job_control.create_info())

        if req_body['req']['api'] == '/job':
            return {"resp": {"code": 200}, "data": {"job_info": info_dto}}

        if req_body['req']['api'] == '/release':
            if 'data' not in req_body:
                return {"resp": {"error": "missing_data"}}
            if 'pending' not in req_body['data']:
                return {"resp": {"error": "missing_data_field", "field": "pending"}}

            if self._latch_release:
                released = self._latch_release.release(req_body.get('data').get('pending'))
            else:
                released = False
            return {"resp": {"code": 200}, "data": {"job_info": info_dto, "released": released}}

        if req_body['req']['api'] == '/stop':
            self._job_control.stop()
            return {"resp": {"code": 200}, "data": {"job_info": info_dto, "result": "stop_performed"}}

        if req_body['req']['api'] == '/interrupt':
            self._job_control.interrupt()
            return {"resp": {"code": 200}, "data": {"job_info": info_dto, "result": "interrupt_performed"}}

        return {"resp": {"error": "unknown_req_api"}}


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

    def stop_jobs(self, instances, interrupt: bool) -> List[Tuple[JobInfo, str]]:
        if not instances:
            raise ValueError('Instances to be stopped cannot be empty')

        inst_responses = self.communicate({'req': {'api': '/interrupt' if interrupt else '/stop'}}, instances)
        return [(_create_job_info(inst_resp), inst_resp.response['data']['result']) for inst_resp in inst_responses]
