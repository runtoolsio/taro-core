from functools import singledispatch
from types import SimpleNamespace

import yaml

from taro import paths

LOG_ENABLED = 'log.enabled'
LOG_STDOUT_LEVEL = 'log.stdout.level'
LOG_FILE_LEVEL = 'log.file.level'
LOG_FILE_PATH = 'log.file.path'


def read_config() -> SimpleNamespace:
    config_file_path = paths.lookup_config_file_path()
    with open(config_file_path, 'r') as file:
        return wrap_namespace(yaml.safe_load(file))


def print_config():
    config_file_path = paths.lookup_config_file_path()
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
