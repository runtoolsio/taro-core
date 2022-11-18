import json
from typing import List, Tuple, Any, Dict

from taro import dto, JobInstanceID
from taro.jobs.api import API_FILE_EXTENSION
from taro.jobs.job import JobInfo
from taro.socket import SocketClient


def read_jobs_info(job_instance="") -> List[JobInfo]:
    with JobsClient() as client:
        return client.read_jobs_info(job_instance)


def release_jobs(pending) -> List[JobInstanceID]:
    with JobsClient() as client:
        return client.release_jobs(pending)


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

    def _send_request(self, api: str, *, data=None, job_instance: str = '', include=()) -> List[Dict[str, Any]]:
        req = {'req': {'api': api}}
        if job_instance:
            req['job_instance'] = job_instance
        if data:
            req['data'] = data

        return [
            json.loads(resp_body) for _, resp_body, error in self.communicate(json.dumps(req), include=include)
            if not error
        ]

    def read_jobs_info(self, job_instance="") -> List[JobInfo]:
        responses = self._send_request('/jobs', job_instance=job_instance)
        return [dto.to_job_info(job['data']['job_info']) for job in _get_jobs(responses)]

    def read_tail(self, job_instance) -> List[Tuple[JobInstanceID, List[str]]]:
        responses = self._send_request('/jobs/tail', job_instance=job_instance)
        return [(_job_instance_id(job), job['data']['tail']) for job in _get_jobs(responses)]

    def release_jobs(self, pending) -> List[JobInstanceID]:
        responses = self._send_request('/jobs/release', data={"pending": pending})
        return [_job_instance_id(job) for job in _get_jobs(responses) if job['data']['released']]

    def stop_jobs(self, instances) -> List[Tuple[JobInstanceID, str]]:
        """

        :param instances:
        :return: list of tuple[instance-id, stop-result]
        """
        if not instances:
            raise ValueError('Instances to be stopped cannot be empty')

        responses = self._send_request('/jobs/stop', include=instances)
        return [(_job_instance_id(job), job['data']['result']) for job in _get_jobs(responses)]


def _job_instance_id(job_resp):
    return JobInstanceID(job_resp['job_id'], job_resp['instance_id'])


def _get_jobs(responses):
    return [job for resp in responses for job in resp['jobs']]
