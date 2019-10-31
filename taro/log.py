import logging
import os

from taro import paths

_root_logger = logging.getLogger()
_root_logger.setLevel(logging.DEBUG)
_formatter = logging.Formatter('%(asctime)s - %(levelname)-5s - %(name)s - %(message)s')


def setup_console(level):
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.getLevelName(level.upper()))
    console_handler.setFormatter(_formatter)
    _root_logger.addHandler(console_handler)


def setup_file(level, file=None):
    file_handler = logging.FileHandler(_expand_user(file) or paths.log_file_path())
    file_handler.setLevel(logging.getLevelName(level.upper()))
    file_handler.setFormatter(_formatter)
    _root_logger.addHandler(file_handler)


def _expand_user(file):
    if file is None or not file.startswith('~'):
        return file

    return os.path.expanduser(file)
