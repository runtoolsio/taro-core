import os
from abc import ABC, abstractmethod
from typing import List, Optional

from taro import util, paths, client
from taro.jobs import persistence
from taro.jobs.job import Job


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

    def read_jobs(self):
        cns = util.read_yaml_file(self.path or paths.lookup_jobs_file())  # TODO read cfg
        jobs = cns.get('jobs')
        if not jobs:
            return []

        return [Job(j.get('id'), vars(j.get('properties'))) for j in jobs]

    def reset(self, overwrite: bool):
        # TODO Create `taro config create --jobs` command for this
        path = self.path or (paths.taro_config_file_search_path(exclude_cwd=True)[0] / paths.JOBS_FILE)
        if not os.path.exists(path) or overwrite:
            util.write_yaml_file(JobRepositoryFile.DEF_FILE_CONTENT, path)


class JobRepositoryActiveInstances(JobRepository):

    @property
    def id(self):
        return 'active'

    def read_jobs(self):
        return {Job(i.job_id) for i in client.read_jobs_info().responses}


class JobRepositoryHistory(JobRepository):

    @property
    def id(self):
        return 'history'

    def read_jobs(self):
        return {Job(s.job_id) for s in persistence.read_stats()}


def _init_repos():
    repos = [JobRepositoryActiveInstances(), JobRepositoryHistory(), JobRepositoryFile()]  # Keep the correct order
    return {repo.id: repo for repo in repos}


_job_repos = _init_repos()


def add_repo(repo):
    _job_repos[repo.id] = repo


def read_job(job_id) -> Optional[Job]:
    for repo in reversed(_job_repos.values()):
        job = repo.read_job(job_id)
        if job:
            return job

    return None


def read_jobs() -> List[Job]:
    jobs = {}
    for repo in _job_repos.values():
        for job in repo.read_jobs():
            jobs[job.id] = job

    return list(jobs.values())
