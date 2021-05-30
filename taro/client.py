from typing import List, Tuple

from taro.jobs.api import Client
from taro.jobs.job import JobInfo


def read_jobs_info(instance="") -> List[JobInfo]:
    with Client() as client:
        return client.read_jobs_info(instance)


def release_jobs(pending):
    with Client() as client:
        client.release_jobs(pending)


def stop_jobs(instances, interrupt: bool) -> List[Tuple[str, str]]:
    with Client() as client:
        return client.stop_jobs(instances, interrupt)


def read_tail(instance) -> List[Tuple[str, str, List[str]]]:
    with Client() as client:
        return client.read_tail(instance)
