import json
import logging
from dataclasses import dataclass
from typing import List, Tuple, Any, Dict, NamedTuple, Optional

from taro import dto
from taro.jobs.api import API_FILE_EXTENSION
from taro.jobs.job import JobInfo, JobInstanceID
from taro.socket import SocketClient, InstanceResponse, Error

log = logging.getLogger(__name__)


class JobInstanceResponse(NamedTuple):
    id: JobInstanceID
    body: Dict[str, Any]


@dataclass
class APIError:
    api_id: str
    socket_error: Optional[Error]
    error: dict[str, Any]


def read_jobs_info(job_instance="") -> Tuple[List[JobInfo], List[APIError]]:
    with JobsClient() as client:
        return client.read_jobs_info(job_instance)


def release_jobs(pending_group) -> Tuple[List[JobInstanceID], List[APIError]]:
    with JobsClient() as client:
        return client.release_jobs(pending_group)


def stop_jobs(instances, interrupt: bool) -> Tuple[List[Tuple[JobInstanceID, str]], List[APIError]]:
    with JobsClient() as client:
        return client.stop_jobs(instances, interrupt)  # TODO ??


def read_tail(instance) -> Tuple[List[Tuple[JobInstanceID, List[str]]], List[APIError]]:
    with JobsClient() as client:
        return client.read_tail(instance)


class JobsClient(SocketClient):

    def __init__(self):
        super().__init__(API_FILE_EXTENSION, bidirectional=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _send_request(self, api: str, req_body=None, *, job_instance: str = '')\
            -> Tuple[List[JobInstanceResponse], List[APIError]]:
        if not req_body:
            req_body = {}
        req_body["request_metadata"] = {"api": api}
        if job_instance:
            req_body["request_metadata"]["match"] = {"ids": [job_instance]}

        server_responses: List[InstanceResponse] = self.communicate(json.dumps(req_body))
        return _process_responses(server_responses)

    def read_jobs_info(self, job_instance="") -> Tuple[List[JobInfo], List[APIError]]:
        instance_responses, api_errors = self._send_request('/jobs', job_instance=job_instance)
        return [dto.to_job_info(body["job_info"]) for _, body in instance_responses], api_errors

    def read_tail(self, job_instance) -> Tuple[List[Tuple[JobInstanceID, List[str]]], List[APIError]]:
        instance_responses, api_errors = self._send_request('/jobs/tail', job_instance=job_instance)
        return [(jid, body["tail"]) for jid, body in instance_responses], api_errors

    def release_jobs(self, pending_group) -> Tuple[List[JobInstanceID], List[APIError]]:
        instance_responses, api_errors = self._send_request('/jobs/release', {"pending_group": pending_group})
        return [jid for jid, body in instance_responses if body["released"]], api_errors

    def stop_jobs(self, instance) -> Tuple[List[Tuple[JobInstanceID, str]], List[APIError]]:
        """

        :param instance:
        :return: list of tuple[instance-id, stop-result]
        """
        if not instance:
            raise ValueError('Instances to be stopped cannot be empty')

        instance_responses, api_errors = self._send_request('/jobs/stop', job_instance=instance)
        return [(jid, body["result"]) for jid, body in instance_responses], api_errors


def _process_responses(responses) -> Tuple[List[JobInstanceResponse], List[APIError]]:
    instance_responses: List[JobInstanceResponse] = []
    api_errors: List[APIError] = []

    for server_id, resp, error in responses:
        if error:
            log.error("event=[response_error] type=[socket] error=[%s]", error)
            api_errors.append(APIError(server_id, error, {}))
        else:
            resp_body = json.loads(resp)
            if "error" in resp_body:
                log.error("event=[response_error] type=[api] error=[%s]", resp_body["error"])
                api_errors.append(APIError(server_id, None, resp_body["error"]))
            else:
                for instance_resp in resp_body['instances']:
                    resp_metadata = instance_resp['response_metadata']
                    jid = JobInstanceID(resp_metadata["job_id"], resp_metadata["instance_id"])
                    instance_responses.append(JobInstanceResponse(jid, instance_resp))

    return instance_responses, api_errors
