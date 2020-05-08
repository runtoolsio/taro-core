import abc

from taro import paths
from taro.persistence.sqlite import SQLite


class _Persistence(abc.ABC):

    @abc.abstractmethod
    def store_ended_job(self, job_info):
        """Store job after finished execution"""

    @abc.abstractmethod
    def close(self):
        """Release resources"""


class _NonePersistence(_Persistence):

    def store_ended_job(self, job_info):
        pass

    def close(self):
        pass


_persistence = _NonePersistence()


def init_sqlite(sqlite_db_path):
    global _persistence
    import sqlite3
    db_con = sqlite3.connect(sqlite_db_path)
    _persistence = SQLite(db_con)


def store_ended_job(self, job_info):
    _persistence.store_ended_job()


def close():
    _persistence.close()
