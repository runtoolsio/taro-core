import logging
from typing import List

from taro import dto
from taro.job import JobInstanceData
from taro.socket import SocketServer, SocketClient
from taro.util import iterates

log = logging.getLogger(__name__)

API_FILE_EXTENSION = '.api'


def _create_socket_name(job_instance):
    return job_instance.instance_id + API_FILE_EXTENSION


class Server(SocketServer):

    def __init__(self, job_control):
        super().__init__(_create_socket_name(job_control))
        self._job_control = job_control

    def handle(self, req_body):
        if 'req' not in req_body:
            return {"resp": {"error": "missing_req"}}
        if 'api' not in req_body['req']:
            return {"resp": {"error": "missing_req_api"}}

        if req_body['req']['api'] == '/job':
            return {"resp": {"code": 200}, "data": {"job_instance": dto.job_instance(self._job_control)}}

        if req_body['req']['api'] == '/release':
            if 'data' not in req_body:
                return {"resp": {"error": "missing_data"}}
            if 'wait' not in req_body['data']:
                return {"resp": {"error": "missing_data_wait"}}

            released = self._job_control.release(req_body.get('data').get('wait'))
            return {"resp": {"code": 200}, "data": {"job_id": self._job_control.job_id, "released": released}}

        return {"resp": {"error": "unknown_req_api"}}


class Client(SocketClient):

    def __init__(self):
        super().__init__(API_FILE_EXTENSION, bidirectional=True)

    def read_jobs_info(self) -> List[JobInstanceData]:
        server = self.servers()
        jobs = []
        while True:
            try:
                next(server)
            except StopIteration:
                break
            resp_body = server.send({'req': {'api': '/job'}})
            jobs.append(dto.to_job_instance_data(resp_body['data']['job_instance']))
        return jobs

    @iterates
    def release_jobs(self, wait):
        server = self.servers()
        while True:
            next(server)
            resp = server.send({'req': {'api': '/release'}, "data": {"wait": wait}})
            if resp['data']['released']:
                print(resp)
