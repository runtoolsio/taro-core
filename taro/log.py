import logging
import sys
from logging import handlers

from taro import paths, cfg
from taro.cfg import LogMode
from taro.util import expand_user

_root_logger = logging.getLogger('taro')
_root_logger.setLevel(logging.DEBUG)
_formatter = logging.Formatter('%(asctime)s - %(levelname)-5s - %(name)s - %(message)s')

STDOUT_HANDLER = 'stdout-handler'
STDERR_HANDLER = 'stderr-handler'
FILE_HANDLER = 'file-handler'


def init(log_mode=LogMode.ENABLED, log_stdout_level='warn', log_file_level='info', log_file_path=None):
    _root_logger.disabled = False  # Resetting required for tests

    if log_mode != LogMode.ENABLED:
        disable()
        return

    if log_stdout_level != 'off':
        setup_console(log_stdout_level)

    if log_file_level != 'off':
        log_file_path = expand_user(log_file_path) or paths.log_file_path(create=True)
        setup_file(log_file_level, log_file_path)


def init_by_config():
    init(cfg.log_mode, cfg.log_stdout_level, cfg.log_file_level, cfg.log_file_path)


def disable():
    _root_logger.disabled = True


def is_disabled():
    return _root_logger.disabled


def setup_console(level):
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.set_name(STDOUT_HANDLER)
    stdout_handler.setLevel(logging.getLevelName(level.upper()))
    stdout_handler.setFormatter(_formatter)
    stdout_handler.addFilter(lambda record: record.levelno <= logging.INFO)
    _register_handler(stdout_handler)

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.set_name(STDERR_HANDLER)
    stderr_handler.setLevel(logging.getLevelName(level.upper()))
    stderr_handler.setFormatter(_formatter)
    stderr_handler.addFilter(lambda record: record.levelno > logging.INFO)
    _register_handler(stderr_handler)


def get_console_level():
    return _get_handler_level(STDOUT_HANDLER)


def setup_file(level, file):
    file_handler = logging.handlers.WatchedFileHandler(file)
    file_handler.set_name(FILE_HANDLER)
    file_handler.setLevel(logging.getLevelName(level.upper()))
    file_handler.setFormatter(_formatter)
    _register_handler(file_handler)


def get_file_level():
    return _get_handler_level(FILE_HANDLER)


def get_file_path():
    handler = _find_handler(FILE_HANDLER)
    if handler:
        return handler.baseFilename
    else:
        return None


def _find_handler(name):
    for handler in _root_logger.handlers:
        if handler.name == name:
            return handler

    return None


def _register_handler(handler):
    previous = _find_handler(handler.name)
    if previous:
        _root_logger.removeHandler(previous)

    _root_logger.addHandler(handler)


def _get_handler_level(name):
    handler = _find_handler(name)
    return handler.level if handler else None
