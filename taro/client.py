from typing import List, Tuple

from taro import JobInfo
from taro.api import Client


def read_jobs_info(instance="") -> List[JobInfo]:
    with Client() as client:
        return client.read_jobs_info(instance)


def release_jobs(pending):
    with Client() as client:
        client.release_jobs(pending)


def stop_jobs(instances, interrupt: bool) -> List[Tuple[str, str]]:
    with Client() as client:
        return client.stop_jobs(instances, interrupt)


def read_tail(instance) -> List[Tuple[str, List[str]]]:
    with Client() as client:
        return client.read_tail(instance)
