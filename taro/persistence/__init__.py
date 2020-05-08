from taro.persistence.sqlite import SQLite

_persistence = None


def init_sqlite(db_connection):
    global _persistence
    sqlite_ = SQLite(db_connection)
    sqlite_.check_tables_exist()
    _persistence = sqlite_


def read_jobs(*, chronological):
    _exc_if_disabled()
    return _persistence.read_jobs(chronological=chronological)


def store_job(job_info):
    _exc_if_disabled()
    _persistence.store_job(job_info)


def _exc_if_disabled():
    if not _persistence:
        raise DisabledError()


class DisabledError(Exception):
    """Raised when persistence is not initialized"""

    def __init__(self):
        super().__init__('Persistence is disabled')
