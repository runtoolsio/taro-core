import importlib
import pkgutil
from enum import Enum

import taro.db
from taro import cfg, ExecutionState, util
from taro import paths


def _load_persistence(type_):
    if not cfg.persistence_enabled:
        return MemoryPersistence()

    for finder, name, is_pkg in pkgutil.iter_modules(taro.db.__path__, taro.db.__name__ + "."):
        if name == taro.db.__name__ + "." + type_:
            db_module = importlib.import_module(name)
            return db_module.create_persistence()

    raise PersistenceNotFoundError(taro.db.__name__ + "." + type_)


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
    from taro.db.sqlite import SQLite

    db_con = sqlite3.connect(cfg.persistence_database or str(paths.sqlite_db_path(True)))
    sqlite_ = SQLite(db_con)
    sqlite_.check_tables_exist()
    return sqlite_


class SortCriteria(Enum):
    CREATED = 1
    FINISHED = 2
    TIME = 3


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


class PersistenceError(Exception):
    pass


class PersistenceNotFoundError(PersistenceError):

    def __init__(self, module_):
        super().__init__(f'Cannot find persistence module {module_}. Ensure the module is installed '
                         f'or check that persistence type value in the config is correct.')


def _sort_key(sort: SortCriteria):
    def key(j):
        if sort == SortCriteria.CREATED:
            return j.lifecycle.changed(ExecutionState.CREATED)
        if sort == SortCriteria.FINISHED:
            return j.lifecycle.execution_finished()
        if sort == SortCriteria.TIME:
            return j.lifecycle.execution_time()
        raise ValueError(sort)

    return key


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


class NoPersistence:

    def read_jobs(self, *, sort, asc, limit):
        raise PersistenceDisabledError()

    def store_job(self, job_info):
        raise PersistenceDisabledError()

    def add_disabled_jobs(self, disabled_jobs):
        raise PersistenceDisabledError()

    def remove_disabled_jobs(self, job_ids):
        raise PersistenceDisabledError()

    def read_disabled_jobs(self):
        raise PersistenceDisabledError()

    def close(self):
        pass


class PersistenceDisabledError(Exception):

    def __init__(self):
        super().__init__('Executed logic depends on data persistence; however, persistence is disabled in the config.')
