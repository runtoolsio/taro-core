from functools import singledispatch
from types import SimpleNamespace

import yaml

from taro import paths


def read_config() -> SimpleNamespace:
    config_file_path = paths.config_file_path()
    with open(config_file_path, 'r') as stream:
        return wrap_namespace(yaml.safe_load(stream))


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
