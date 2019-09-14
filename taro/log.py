import argparse
import logging
import pathlib

_root_logger = logging.getLogger()
_root_logger.setLevel(logging.DEBUG)
_formatter = logging.Formatter('%(asctime)s - %(levelname)-5s - %(name)s - %(message)s')


def configure(args):
    for values in args.log:
        log_type = values[0].lower()
        level = values[1] if len(values) > 1 else None

        if 'file' == log_type:
            _raise_error_on_unknown_values(values, 3)

            file = values[2] if len(values) > 2 else _get_log_file_path()
            _configure_file(level, file)
        else:
            _raise_error_on_unknown_values(values, 2)

            if 'stdout' == log_type:
                _configure_console(level)


def _raise_error_on_unknown_values(values, max_values):
    if len(values) > max_values:
        raise argparse.ArgumentError(None, "Argument --log has unknown values: " + ' '.join(values[max_values:]))


def _get_log_file_path():
    home = pathlib.Path.home()
    path = home / '.cache' / 'taro'
    path.mkdir(parents=True, exist_ok=True)
    return str(path / 'taro.log')


def _configure_console(level):
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level or logging.WARN)
    console_handler.setFormatter(_formatter)
    _root_logger.addHandler(console_handler)


def _configure_file(level, file_path):
    file_handler = logging.FileHandler(file_path)
    file_handler.setLevel(level or logging.INFO)
    file_handler.setFormatter(_formatter)
    _root_logger.addHandler(file_handler)
