"""
Well the name is quite oxymoron. There is no persistence. Used for testing only.
"""

from taro import util
from taro.persistence.common import _sort_key


class MemoryPersistence:

    def __init__(self):
        self._jobs = []
        self._disabled_jobs = []

    def read_jobs(self, *, sort, asc, limit):
        return util.sequence_view(self._jobs, sort_key=_sort_key(sort), asc=asc, limit=limit)

    def store_job(self, job_info):
        self._jobs.append(job_info)

    def add_disabled_jobs(self, disabled_jobs):
        to_add = [d for d in disabled_jobs if not any(d.job_id == j.job_id for j in self._disabled_jobs)]
        self._disabled_jobs += to_add
        return to_add

    def remove_disabled_jobs(self, job_ids):
        removed = []
        for job_id in job_ids:
            try:
                self._disabled_jobs.remove(job_id)
                removed.append(job_id)
            except ValueError:
                continue
        return removed

    def read_disabled_jobs(self):
        return self._disabled_jobs

    def close(self):
        pass
