import json
import logging
from json import JSONDecodeError

from taro import dto
from taro.socket import SocketServer

log = logging.getLogger(__name__)

API_FILE_EXTENSION = '.api'


def _create_socket_name(job_info):
    return job_info.instance_id + API_FILE_EXTENSION


class Server(SocketServer):

    def __init__(self, job_instance, latch_release):
        super().__init__(_create_socket_name(job_instance))
        self._job_instance = job_instance
        self._latch_release = latch_release

    def handle(self, req):
        try:
            req_body = json.loads(req)
        except JSONDecodeError as e:
            log.warning(f"event=[invalid_json_request_body] length=[{e}]")
            return self._resp_err(400, "invalid_req_body")

        if 'req' not in req_body:
            return self._resp_err(422, "missing_req")
        if 'api' not in req_body['req']:
            return self._resp_err(422, "missing_req_api")

        job_instance_filter = req_body.get('job_instance')
        if job_instance_filter and not self._job_instance.create_info().matches(job_instance_filter):
            return self._resp(412, {"reason": "job_instance_not_matching"})

        if req_body['req']['api'] == '/job':
            info_dto = dto.to_info_dto(self._job_instance.create_info())
            return self._resp_ok({"job_info": info_dto})

        if req_body['req']['api'] == '/release':
            if 'data' not in req_body:
                return self._resp_err(422, "missing_data")
            if 'pending' not in req_body['data']:
                return self._resp_err(422, "missing_data_field:pending")

            if self._latch_release:
                released = self._latch_release.release(req_body.get('data').get('pending'))
            else:
                released = False
            return self._resp_ok({"released": released})

        if req_body['req']['api'] == '/stop':
            self._job_instance.stop()
            return self._resp_ok({"result": "stop_performed"})

        if req_body['req']['api'] == '/tail':
            return self._resp_ok({"tail": self._job_instance.last_output})

        return self._resp_err(404, "req_api_not_found")

    def _resp_ok(self, data):
        return self._resp(200, data)

    def _resp(self, code: int, data):
        resp = {
            "resp": {"code": code},
            "job_id": self._job_instance.job_id,
            "instance_id": self._job_instance.instance_id,
            "data": data
        }
        return json.dumps(resp)

    def _resp_err(self, code: int, error: str):
        if 400 > code >= 600:
            raise ValueError("Error code must be 4xx or 5xx")

        err_resp = {
            "resp": {"code": code},
            "job_id": self._job_instance.job_id,
            "instance_id": self._job_instance.instance_id,
            "error": error
        }

        return json.dumps(err_resp)
