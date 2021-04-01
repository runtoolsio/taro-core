from taro import cfg
from taro import paths
from taro.persistence.common import SortCriteria
from taro.persistence.memory import MemoryPersistence
from taro.persistence.none import NoPersistence
from taro.persistence.sqlite import SQLite


def _load_persistence(type_):
    if type_ == 'sqlite':
        return _init_sqlite()
    return MemoryPersistence()


class PersistenceHolder(dict):

    def __missing__(self, key):
        self.close()

        new_instance = _load_persistence(key)
        self[key] = new_instance
        return new_instance

    def close(self):
        for instance in self.values():
            instance.close()
        self.clear()


_persistence = PersistenceHolder()


def _instance():
    return _persistence[cfg.persistence_type]


def _init_sqlite():
    import sqlite3
    from taro.persistence.sqlite import SQLite

    db_con = sqlite3.connect(cfg.persistence_database or str(paths.sqlite_db_path(True)))
    sqlite_ = SQLite(db_con)
    sqlite_.check_tables_exist()
    return sqlite_


def read_jobs(*, sort=SortCriteria.CREATED, asc=False, limit=-1):
    return _instance().read_jobs(sort=sort, asc=asc, limit=limit)


def store_job(job_info):
    _instance().store_job(job_info)


def add_disabled_jobs(disabled_jobs):
    return _instance().add_disabled_jobs(disabled_jobs)


def remove_disabled_jobs(job_ids):
    return _instance().remove_disabled_jobs(job_ids)


def read_disabled_jobs():
    return _instance().read_disabled_jobs()


def close():
    _persistence.close()
