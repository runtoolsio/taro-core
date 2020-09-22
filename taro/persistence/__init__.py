import itertools

from taro import cnf, ExecutionState
from taro import paths


class NoPersistence:

    def __init__(self):
        self._jobs = []
        self._disabled_jobs = []

    def read_jobs(self, *, chronological, limit):
        sorted_jobs = sorted(self._jobs, key=lambda j: j.lifecycle.changed(ExecutionState.CREATED),
                             reverse=not chronological)
        return itertools.islice(sorted_jobs, 0, limit if limit > 0 else None)

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


_persistence = NoPersistence()


def init():
    global _persistence

    if cnf.config.persistence_enabled:  # TODO check is sqlite
        import sqlite3
        from taro.persistence.sqlite import SQLite

        db_con = sqlite3.connect(cnf.config.persistence_database or str(paths.sqlite_db_path(True)))
        sqlite_ = SQLite(db_con)
        sqlite_.check_tables_exist()
        _persistence = sqlite_
        return True
    else:
        disable()
        return False


def disable():
    global _persistence
    _persistence.close()
    _persistence = NoPersistence()


def read_jobs(*, chronological=False, limit=-1):
    return _persistence.read_jobs(chronological=chronological, limit=limit)


def store_job(job_info):
    _persistence.store_job(job_info)


def add_disabled_jobs(disabled_jobs):
    return _persistence.add_disabled_jobs(disabled_jobs)


def remove_disabled_jobs(job_ids):
    return _persistence.remove_disabled_jobs(job_ids)


def read_disabled_jobs():
    return _persistence.read_disabled_jobs()


def close():
    _persistence.close()
