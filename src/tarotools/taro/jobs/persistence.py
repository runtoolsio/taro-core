import importlib
import pkgutil
import sys
from enum import Enum

from tarotools import taro
from tarotools.taro import paths
from tarotools.taro import util, cfg
from tarotools.taro.err import TaroException
from tarotools.taro.jobs.execution import ExecutionState


def is_enabled():
    return cfg.persistence_enabled


def _load_persistence(type_):
    if not cfg.persistence_enabled:
        return NoPersistence()

    for finder, name, is_pkg in pkgutil.iter_modules(taro.jobs.db.__path__, taro.jobs.db.__name__ + "."):
        if name == taro.jobs.db.__name__ + "." + type_:
            db_module = importlib.import_module(name)
            return db_module.create_persistence()

    raise PersistenceNotFoundError(taro.jobs.db.__name__ + "." + type_)


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


def reset():
    _persistence.close()


def _instance():
    if not cfg.persistence_enabled:
        return NoPersistence()
    return _persistence[cfg.persistence_type]


class SortCriteria(Enum):
    CREATED = 1
    ENDED = 2
    TIME = 3


def read_instances(instance_match=None, sort=SortCriteria.ENDED, *, asc=True, limit=-1, offset=-1, last=False):
    return _instance().read_instances(instance_match, sort, asc=asc, limit=limit, offset=offset, last=last)


def read_stats(instance_match=None):
    return _instance().read_stats(instance_match)


def count_instances(instance_match):
    return sum(s.count for s in (_instance().read_stats(instance_match)))


def store_instances(*job_info):
    _instance().store_instances(*job_info)
    clean_up()


def remove_instances(instance_match):
    _instance().remove_instances(instance_match)


def clean_up():
    try:
        max_age = util.parse_iso8601_duration(cfg.persistence_max_age) if cfg.persistence_max_age else None
    except ValueError:
        sys.stderr.write("Invalid max_age in " + str(paths.lookup_config_file()) + "\n")
        return
    _instance().clean_up(cfg.persistence_max_records, max_age)


def close():
    _persistence.close()


def _sort_key(sort: SortCriteria):
    def key(j):
        if sort == SortCriteria.CREATED:
            return j.lifecycle.changed_at(ExecutionState.CREATED)
        if sort == SortCriteria.ENDED:
            return j.lifecycle.ended_at
        if sort == SortCriteria.TIME:
            return j.lifecycle.execution_time
        raise ValueError(sort)

    return key


class NoPersistence:

    def read_instances(self, instance_match=None, sort=SortCriteria.CREATED, *, asc, limit, offset, last=False):
        raise PersistenceDisabledError()

    def read_stats(self, instance_match=None):
        raise PersistenceDisabledError()

    def store_instances(self, job_info):
        raise PersistenceDisabledError()

    def remove_instances(self, instance_match):
        raise PersistenceDisabledError()

    def clean_up(self, max_records, max_age):
        raise PersistenceDisabledError()

    def close(self):
        pass


class PersistenceError(TaroException):
    pass


class PersistenceNotFoundError(PersistenceError):

    def __init__(self, module_):
        super().__init__(f'Cannot find persistence module {module_}. Ensure the module is installed '
                         f'or check that persistence type value in the config is correct.')


class PersistenceDisabledError(PersistenceError):

    def __init__(self):
        super().__init__("Logic execution relies on data persistence, but it's disabled in the config.")