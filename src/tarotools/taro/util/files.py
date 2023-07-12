import os
from pathlib import Path
from shutil import copy

import yaml

from tarotools.taro.util import ns
from tarotools.taro.util.ns import NestedNamespace


def expand_user(file):
    if not isinstance(file, str) or not file.startswith('~'):
        return file

    return os.path.expanduser(file)


def print_file(path):
    path = expand_user(path)
    print('Showing file: ' + str(path))
    with open(path, 'r') as file:
        print(file.read())


def read_yaml_file(file_path) -> NestedNamespace:
    with open(file_path, 'r') as file:
        return ns.wrap_namespace(yaml.safe_load(file))


def write_yaml_file(content, file_path):
    with open(file_path, 'w') as file:
        return yaml.dump(content, file)


def copy_resource(src: Path, dst: Path, overwrite=False):
    if not dst.parent.is_dir():
        os.makedirs(dst.parent)

    if not dst.exists() or overwrite:
        copy(src, dst)

    raise FileExistsError('File already exists: ' + str(dst))
