from taro import cfg
from taro import paths
from taro.persistence.common import SortCriteria
from taro.persistence.memory import MemoryPersistence

_persistence = MemoryPersistence()


def init():
    global _persistence

    if cfg.persistence_enabled:  # TODO check is sqlite
        import sqlite3
        from taro.persistence.sqlite import SQLite

        db_con = sqlite3.connect(cfg.persistence_database or str(paths.sqlite_db_path(True)))
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
    _persistence = MemoryPersistence()


def read_jobs(*, sort=SortCriteria.CREATED, asc=False, limit=-1):
    return _persistence.read_jobs(sort=sort, asc=asc, limit=limit)


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
