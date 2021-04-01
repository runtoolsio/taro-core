from taro import cfg
from taro import paths
from taro.cfg import PersistenceType
from taro.persistence.common import SortCriteria
from taro.persistence.memory import MemoryPersistence
from taro.persistence.none import NoPersistence
from taro.persistence.sqlite import SQLite

_persistence = None


def _init_sqlite():
    import sqlite3
    from taro.persistence.sqlite import SQLite

    db_con = sqlite3.connect(cfg.persistence_database or str(paths.sqlite_db_path(True)))
    sqlite_ = SQLite(db_con)
    sqlite_.check_tables_exist()
    return sqlite_


_CFG2TYPE = {
    PersistenceType.SQL_LITE: SQLite,
}

_CFG2INIT = {
    PersistenceType.SQL_LITE: _init_sqlite,
}


def _is_setup():
    if not cfg.persistence_enabled:
        return isinstance(_persistence, MemoryPersistence)
    return isinstance(_persistence, _CFG2TYPE.get(cfg.persistence_type))


def _instance():
    global _persistence
    if not _is_setup():
        if _persistence:
            _persistence.close()
        if cfg.persistence_enabled:
            _persistence = _CFG2INIT[cfg.persistence_type]()
        else:
            _persistence = MemoryPersistence()

    return _persistence


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
    _instance().close()
    global _persistence
    _persistence = MemoryPersistence()
