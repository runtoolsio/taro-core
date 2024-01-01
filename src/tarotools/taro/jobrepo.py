"""
This module serves as a source of job definitions and offers:
 1. An API to read job definitions, with each represented as an instance of the `Job` class, from job repositories.
 2. A job repository interface.
 3. Default job repositories: active, history, and file.

To add a custom job repository, implement the `JobRepository` interface and pass its instance to the `add_repo` function
"""


import os
from abc import ABC, abstractmethod
from typing import List, Optional

from tarotools.taro import paths, persistence
from tarotools.taro import util, client
from tarotools.taro.job import Job
from tarotools.taro.persistence import PersistenceDisabledError


class JobRepository(ABC):

    @property
    @abstractmethod
    def id(self):
        pass

    @abstractmethod
    def read_jobs(self):
        pass

    def read_job(self, job_id):
        for job in self.read_jobs():
            if job.id == job_id:
                return job

        return None


class JobRepositoryFile(JobRepository):
    DEF_FILE_CONTENT = \
        {
            'jobs': [
                {
                    'id': '_this_is_example_taro_job_',
                    'properties': {'prop1': 'value1'}
                }
            ]
        }

    def __init__(self, path=None):
        self.path = path

    @property
    def id(self):
        return 'file'

    def read_jobs(self) -> List[Job]:
        root = util.read_toml_file(self.path or paths.lookup_jobs_file())
        jobs = root.get('jobs')
        if not jobs:
            return []

        return [Job(j.get('id'), j.get('properties')) for j in jobs]

    def reset(self, overwrite: bool):
        # TODO Create `runcore config create --jobs` command for this
        path = self.path or (paths.taro_config_file_search_path(exclude_cwd=True)[0] / paths.JOBS_FILE)
        if not os.path.exists(path) or overwrite:
            pass
            # TODO Copy file from resources
            # util.write_yaml_file(JobRepositoryFile.DEF_FILE_CONTENT, path)


class JobRepositoryActiveInstances(JobRepository):

    @property
    def id(self):
        return 'active'

    def read_jobs(self) -> List[Job]:
        return [*{Job(i.job_id) for i in client.get_active_runs().responses}]


class JobRepositoryHistory(JobRepository):

    @property
    def id(self):
        return 'history'

    def read_jobs(self) -> List[Job]:
        try:
            return [*{Job(s.job_id) for s in persistence.read_stats()}]
        except PersistenceDisabledError:
            return []


def _init_repos():
    repos = [JobRepositoryActiveInstances(), JobRepositoryHistory(), JobRepositoryFile()]  # Keep the correct order
    return {repo.id: repo for repo in repos}


_job_repos = _init_repos()


def add_repo(repo):
    _job_repos[repo.id] = repo


def read_job(job_id) -> Optional[Job]:
    """
    Args:
        job_id (str): ID of the job to be searched.

    Returns:
        Optional[Job]: Searched job from registered repositories or None if not found.
    """
    for repo in reversed(_job_repos.values()):
        job = repo.read_job(job_id)
        if job:
            return job

    return None


def read_jobs() -> List[Job]:
    """
    Returns:
        List[Job]: Jobs from all registered repositories
    """
    jobs = {}
    for repo in _job_repos.values():
        for job in repo.read_jobs():
            jobs[job.id] = job

    return list(jobs.values())
