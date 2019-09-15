import logging
import pathlib

_root_logger = logging.getLogger()
_root_logger.setLevel(logging.DEBUG)
_formatter = logging.Formatter('%(asctime)s - %(levelname)-5s - %(name)s - %(message)s')


def _setup_console(level):
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level or logging.WARN)
    console_handler.setFormatter(_formatter)
    _root_logger.addHandler(console_handler)


def setup_file(level):
    file_handler = logging.FileHandler(_get_log_file_path())
    file_handler.setLevel(logging.getLevelName(level.upper()))
    file_handler.setFormatter(_formatter)
    _root_logger.addHandler(file_handler)


def _get_log_file_path():
    """TODO root"""
    home = pathlib.Path.home()
    path = home / '.cache' / 'taro'
    path.mkdir(parents=True, exist_ok=True)
    return str(path / 'taro.log')
