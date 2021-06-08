from typing import List, Tuple

from taro import dto
from taro.jobs.api import API_FILE_EXTENSION
from taro.jobs.job import JobInfo
from taro.socket import SocketClient, InstanceResponse
from taro.util import iterates


def read_jobs_info(instance="") -> List[JobInfo]:
    with JobsClient() as client:
        return client.read_jobs_info(instance)


def release_jobs(pending):
    with JobsClient() as client:
        client.release_jobs(pending)


def stop_jobs(instances, interrupt: bool) -> List[Tuple[str, str]]:
    with JobsClient() as client:
        return client.stop_jobs(instances, interrupt)


def read_tail(instance) -> List[Tuple[str, str, List[str]]]:
    with JobsClient() as client:
        return client.read_tail(instance)


class JobsClient(SocketClient):

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


def _create_job_info(info_resp):
    return dto.to_job_info(info_resp.response['data']['job_info'])
