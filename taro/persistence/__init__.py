from taro.persistence.sqlite import SQLite

_persistence = None


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


class NoPersistence:

    def __init__(self):
        self._jobs = []

    def read_jobs(self, *, chronological):
        return list(reversed(self._jobs)) if chronological else self._jobs

    def store_job(self, job_info):
        self._jobs.append(job_info)
