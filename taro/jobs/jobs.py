import os
from typing import List

from taro import util, paths
from taro.jobs.job import Job


def get_job(id) -> Job:
    cns = util.read_yaml_file(paths.lookup_jobs_file())
    _job = cns.get(id)
    return Job(vars(_job.properties), id) if _job else None


def get_all_jobs() -> List[Job]:
    cns = util.read_yaml_file(paths.lookup_jobs_file())
    return [Job(vars(getattr(cns, id).properties), id) for id in vars(cns)]


def create_jobs_file(overwrite: bool):
    path = paths.config_file_search_path(exclude_cwd=True)[0] / paths.JOBS_FILE
    if not os.path.exists(path) or overwrite:
        open(path, 'w').close()