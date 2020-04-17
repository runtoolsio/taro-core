from collections import Iterable
from functools import singledispatch
from types import SimpleNamespace

import yaml

from taro.util import get_attr

LOG_ENABLED = 'log.enabled'
LOG_STDOUT_LEVEL = 'log.stdout.level'
LOG_FILE_LEVEL = 'log.file.level'
LOG_FILE_PATH = 'log.file.path'
PERSISTENCE_ENABLED = 'persistence.enabled'
PERSISTENCE_TYPE = 'persistence.type'
PLUGINS = 'plugins'


def read_config(config_file_path) -> SimpleNamespace:
    with open(config_file_path, 'r') as file:
        return wrap_namespace(yaml.safe_load(file))


def print_config(config_file_path):
    print('Showing config file: ' + str(config_file_path))
    with open(config_file_path, 'r') as file:
        print(file.read())


# Martijn Pieters' solution below: https://stackoverflow.com/questions/50490856
@singledispatch
def wrap_namespace(ob):
    return ob


@wrap_namespace.register(dict)
def _wrap_dict(ob):
    return SimpleNamespace(**{k: wrap_namespace(v) for k, v in ob.items()})


@wrap_namespace.register(list)
def _wrap_list(ob):
    return [wrap_namespace(v) for v in ob]


class Config:

    def __init__(self, cns):
        self.log_enabled = get_attr(cns, LOG_ENABLED, default=True)
        self.log_stdout_level = get_attr(cns, LOG_STDOUT_LEVEL, default='off', type_=str).lower()
        self.log_file_level = get_attr(cns, LOG_FILE_LEVEL, default='off', type_=str).lower()
        self.log_file_path = get_attr(cns, LOG_FILE_PATH, type_=str)

        self.persistence_enabled = get_attr(cns, PERSISTENCE_ENABLED, default=False)
        plugins = get_attr(cns, PLUGINS)
        if isinstance(plugins, str):
            self.plugins = (plugins,)
        elif isinstance(plugins, Iterable):
            self.plugins = tuple(plugins)
        else:
            self.plugins = ()
