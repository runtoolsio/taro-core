import logging
from typing import List, Tuple

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

        instance = dto.job_instance(self._job_control)

        if req_body['req']['api'] == '/job':
            return {"resp": {"code": 200}, "data": {"job_instance": instance}}

        if req_body['req']['api'] == '/release':
            if 'data' not in req_body:
                return {"resp": {"error": "missing_data"}}
            if 'wait' not in req_body['data']:
                return {"resp": {"error": "missing_data_wait"}}

            released = self._job_control.release(req_body.get('data').get('wait'))
            return {"resp": {"code": 200}, "data": {"job_instance": instance, "released": released}}

        if req_body['req']['api'] == '/stop':
            self._job_control.stop()
            return {"resp": {"code": 200}, "data": {"job_instance": instance, "result": "stop_performed"}}

        return {"resp": {"error": "unknown_req_api"}}


def _create_job_instance(inst_resp):
    return dto.to_job_instance_data(inst_resp.response['data']['job_instance'])


class Client(SocketClient):

    def __init__(self):
        super().__init__(API_FILE_EXTENSION, bidirectional=True)

    def read_jobs_info(self) -> List[JobInstanceData]:
        responses = self.communicate({'req': {'api': '/job'}})
        return [_create_job_instance(inst_resp) for inst_resp in responses]

    @iterates
    def release_jobs(self, wait):
        server = self.servers()
        while True:
            next(server)
            resp = server.send({'req': {'api': '/release'}, "data": {"wait": wait}}).response
            if resp['data']['released']:
                print(resp)  # TODO Do not print, but returned released (use communicate)

    def stop_jobs(self, instances) -> List[Tuple[JobInstanceData, str]]:
        if not instances:
            raise ValueError('Instances to be stopped cannot be empty')

        inst_responses = self.communicate({'req': {'api': '/stop'}}, instances)
        return [(_create_job_instance(inst_resp), inst_resp.response['data']['result']) for inst_resp in inst_responses]
