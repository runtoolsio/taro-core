from taro.persistence.sqlite import SQLite


class NoPersistence:

    def __init__(self):
        self._jobs = []
        self._disabled = []

    def read_jobs(self, *, chronological):
        return list(reversed(self._jobs)) if chronological else self._jobs

    def store_job(self, job_info):
        self._jobs.append(job_info)

    def add_disabled_jobs(self, job_ids):
        self._disabled += job_ids

    def read_disabled_jobs(self):
        return self._disabled


_persistence = NoPersistence()


def disable():
    global _persistence
    _persistence = NoPersistence()


def init_sqlite(db_connection):
    global _persistence
    sqlite_ = SQLite(db_connection)
    sqlite_.check_tables_exist()
    _persistence = sqlite_


def read_jobs(*, chronological):
    return _persistence.read_jobs(chronological=chronological)


def store_job(job_info):
    _persistence.store_job(job_info)


def add_disabled_jobs(job_ids):
    _persistence.add_disabled_jobs(job_ids)


def read_disabled_jobs():
    return _persistence.read_disabled_jobs()
