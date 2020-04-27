from collections.abc import Iterable
from types import SimpleNamespace

import yaml

from taro.util import wrap_namespace

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


class Config:

    def __init__(self, cns):
        self.log_enabled = cns.get(LOG_ENABLED, default=True)
        self.log_stdout_level = cns.get(LOG_STDOUT_LEVEL, default='off', type_=str).lower()
        self.log_file_level = cns.get(LOG_FILE_LEVEL, default='off', type_=str).lower()
        self.log_file_path = cns.get(LOG_FILE_PATH, type_=str)

        self.persistence_enabled = cns.get(PERSISTENCE_ENABLED, default=False)
        plugins = cns.get(PLUGINS)
        if isinstance(plugins, str):
            self.plugins = (plugins,)
        elif isinstance(plugins, Iterable):
            self.plugins = tuple(plugins)
        else:
            self.plugins = ()
