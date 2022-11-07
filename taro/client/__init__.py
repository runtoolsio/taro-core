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
            resp
            for _, resp_body, error in self.communicate(json.dumps(req), include=include)
            if not error and (resp := json.loads(resp_body))['resp']['code'] != 412  # Ignore precondition failed
        ]

    def read_jobs_info(self, job_instance="") -> List[JobInfo]:
        responses = self._send_request('/job', job_instance=job_instance)
        return [dto.to_job_info(resp['data']['job_info']) for resp in responses]

    def read_tail(self, job_instance) -> List[Tuple[JobInstanceID, List[str]]]:
        responses = self._send_request('/tail', job_instance=job_instance)
        return [(_job_instance_id(resp), resp['data']['tail']) for resp in responses]

    def release_jobs(self, pending) -> List[JobInstanceID]:
        responses = self._send_request('/release', data={"pending": pending})
        return [_job_instance_id(resp) for resp in responses if resp['data']['released']]

    def stop_jobs(self, instances) -> List[Tuple[JobInstanceID, str]]:
        """

        :param instances:
        :return: list of tuple[instance-id, stop-result]
        """
        if not instances:
            raise ValueError('Instances to be stopped cannot be empty')

        responses = self._send_request('/stop', include=instances)
        return [(_job_instance_id(resp), resp['data']['result']) for resp in responses]


def _job_instance_id(resp):
    return JobInstanceID(resp['job_id'], resp['instance_id'])
