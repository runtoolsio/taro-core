import os
from typing import List

from taro import util, paths
from taro.jobs.inst import Job

JOBS_FILE_CONTENT =\
    {
        'jobs': [
            {
                'job_id': '_this_is_example_taro_job_',
                'properties': {'prop1': 'value1'}
            }
        ]
    }


def get_job(job_id) -> Job:
    id_job = {j.job_id: j for j in get_all_jobs()}
    return id_job.get(job_id)


def get_all_jobs() -> List[Job]:
    cns = util.read_yaml_file(paths.lookup_jobs_file())
    jobs = cns.get('jobs')
    if not jobs:
        return []

    return [Job(j.get('job_id'), vars(j.get('properties'))) for j in jobs]


def reset(overwrite: bool):
    # TODO Create `taro config create --jobs` command for this
    path = paths.taro_config_file_search_path(exclude_cwd=True)[0] / paths.JOBS_FILE
    if not os.path.exists(path) or overwrite:
        util.write_yaml_file(JOBS_FILE_CONTENT, path)
