import logging
from typing import List, Tuple

from taro import dto
from taro.job import JobInfo
from taro.socket import SocketServer, SocketClient, InstanceResponse
from taro.util import iterates

log = logging.getLogger(__name__)

API_FILE_EXTENSION = '.api'


def _create_socket_name(job_info):
    return job_info.instance_id + API_FILE_EXTENSION


def _resp(code: int, job_instance: Tuple[str, str], data):
    return {"resp": {"code": code}, "job_id": job_instance[0], "instance_id": job_instance[1], "data": data}


def _resp_err(code: int, job_instance: Tuple[str, str], error: str):
    if 400 > code >= 600:
        raise ValueError("Error code must be 4xx or 5xx")
    return {"resp": {"code": code}, "job_id": job_instance[0], "instance_id": job_instance[1], "error": error}


class Server(SocketServer):

    def __init__(self, job_control, latch_release):
        super().__init__(_create_socket_name(job_control))
        self._job_control = job_control
        self._latch_release = latch_release

    def handle(self, req_body):
        job_inst = (self._job_control.job_id, self._job_control.instance_id)

        if 'req' not in req_body:
            return _resp_err(422, job_inst, "missing_req")
        if 'api' not in req_body['req']:
            return _resp_err(422, job_inst, "missing_req_api")

        inst_filter = req_body.get('instance')
        if inst_filter and not self._job_control.create_info().matches(inst_filter):
            return _resp(412, job_inst, {"reason": "instance_not_matching"})

        if req_body['req']['api'] == '/job':
            info_dto = dto.to_info_dto(self._job_control.create_info())
            return _resp(200, job_inst, {"job_info": info_dto})

        if req_body['req']['api'] == '/release':
            if 'data' not in req_body:
                return _resp_err(422, job_inst, "missing_data")
            if 'pending' not in req_body['data']:
                return _resp_err(422, job_inst, "missing_data_field:pending")

            if self._latch_release:
                released = self._latch_release.release(req_body.get('data').get('pending'))
            else:
                released = False
            return _resp(200, job_inst, {"released": released})

        if req_body['req']['api'] == '/stop':
            self._job_control.stop()
            return _resp(200, job_inst, {"result": "stop_performed"})

        if req_body['req']['api'] == '/interrupt':
            self._job_control.interrupt()
            return _resp(200, job_inst, {"result": "interrupt_performed"})

        if req_body['req']['api'] == '/tail':
            return _resp(200, job_inst, {"tail": self._job_control.last_output})

        return _resp_err(404, job_inst, "req_api_not_found")


def _create_job_info(info_resp):
    return dto.to_job_info(info_resp.response['data']['job_info'])


class Client(SocketClient):

    def __init__(self):
        super().__init__(API_FILE_EXTENSION, bidirectional=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _send_request(self, api: str, *, data=None, instance: str = '', include=()) -> List[InstanceResponse]:
        req = {'req': {'api': api}}
        if instance:
            req['instance'] = instance
        if data:
            req['data'] = data
        return [inst_resp for inst_resp in self.communicate(req, include=include)
                if inst_resp.response['resp']['code'] != 412]  # Ignore precondition failed

    def read_jobs_info(self, instance="") -> List[JobInfo]:
        responses = self._send_request('/job', instance=instance)
        return [_create_job_info(inst_resp) for inst_resp in responses]

    def read_tail(self, instance) -> List[Tuple[str, str, List[str]]]:
        inst_responses = self._send_request('/tail', instance=instance)
        return [(resp['job_id'], resp['instance_id'], resp['data']['tail'])
                for resp in [inst_resp.response for inst_resp in inst_responses]]

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

        inst_responses = self._send_request('/interrupt' if interrupt else '/stop', include=instances)
        return [(resp['job_id'] + "@" + resp['instance_id'], resp['data']['result'])
                for resp in [inst_resp.response for inst_resp in inst_responses]]
