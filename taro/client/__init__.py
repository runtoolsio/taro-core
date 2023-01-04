import json
from typing import List, Tuple, Any, Dict

from taro import dto
from taro.jobs.api import API_FILE_EXTENSION
from taro.jobs.job import JobInfo, JobInstanceID
from taro.socket import SocketClient


def read_jobs_info(job_instance="") -> List[JobInfo]:
    with JobsClient() as client:
        return client.read_jobs_info(job_instance)


def release_jobs(pending_group) -> List[JobInstanceID]:
    with JobsClient() as client:
        return client.release_jobs(pending_group)


def stop_jobs(instances, interrupt: bool) -> List[Tuple[JobInstanceID, str]]:
    with JobsClient() as client:
        return client.stop_jobs(instances, interrupt)  # TODO ??


def read_tail(instance) -> List[Tuple[JobInstanceID, List[str]]]:
    with JobsClient() as client:
        return client.read_tail(instance)


class JobsClient(SocketClient):

    def __init__(self):
        super().__init__(API_FILE_EXTENSION, bidirectional=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _send_request(self, api: str, *, data=None, job_instance: str = '') -> List[Dict[str, Any]]:
        req = {'req': {'api': api}}
        if job_instance:
            req['job_instance'] = job_instance
        if data:
            req['data'] = data

        return [json.loads(resp_body) for _, resp_body, error in self.communicate(json.dumps(req)) if not error]

    def read_jobs_info(self, job_instance="") -> List[JobInfo]:
        responses = self._send_request('/jobs', job_instance=job_instance)
        return [dto.to_job_info(resp['job_info']) for resp in _instance_responses(responses)]

    def read_tail(self, job_instance) -> List[Tuple[JobInstanceID, List[str]]]:
        responses = self._send_request('/jobs/tail', job_instance=job_instance)
        return [(_job_instance_id(resp), resp['tail']) for resp in _instance_responses(responses)]

    def release_jobs(self, pending_group) -> List[JobInstanceID]:
        responses = self._send_request('/jobs/release', data={"pending_group": pending_group})
        return [_job_instance_id(resp) for resp in _instance_responses(responses) if resp['released']]

    def stop_jobs(self, instance) -> List[Tuple[JobInstanceID, str]]:
        """

        :param instance:
        :return: list of tuple[instance-id, stop-result]
        """
        if not instance:
            raise ValueError('Instances to be stopped cannot be empty')

        responses = self._send_request('/jobs/stop', job_instance=instance)
        return [(_job_instance_id(resp), resp['result']) for resp in _instance_responses(responses)]


def _job_instance_id(inst_resp):
    resp_metadata = inst_resp['response_metadata']
    return JobInstanceID(resp_metadata['job_id'], resp_metadata['instance_id'])


def _instance_responses(responses):
    return [instance_resp for resp in responses for instance_resp in resp['instances']]  # TODO Handle errors
