import os
from pathlib import Path
from shutil import copy
from typing import Dict, Any

import tomli

from tarotools.taro.util import flatten_dict


def expand_user(file):
    if not isinstance(file, str) or not file.startswith('~'):
        return file

    return os.path.expanduser(file)


def print_file(path):
    path = expand_user(path)
    print('Showing file: ' + str(path))
    with open(path, 'r') as file:
        print(file.read())


def read_toml_file(file_path) -> Dict[str, Any]:
    with open(file_path, 'rb') as file:
        return tomli.load(file)


def read_toml_file_flatten(file_path) -> Dict[str, Any]:
    with open(file_path, 'rb') as file:
        return flatten_dict(tomli.load(file))


def copy_resource(src: Path, dst: Path, overwrite=False):
    if not dst.parent.is_dir():
        os.makedirs(dst.parent)

    if not dst.exists() or overwrite:
        copy(src, dst)

    raise FileExistsError('File already exists: ' + str(dst))
