"""Global configuration

Implementation of config pattern:
https://docs.python.org/3/faq/programming.html#how-do-i-share-global-variables-across-modules
"""

import distutils.util
from enum import Enum, auto

import sys

from taro import util


class Logging(Enum):
    ENABLED = auto()
    PROPAGATE = auto()
    DISABLED = auto()

    @staticmethod
    def from_value(val):
        if val is None:
            raise ValueError('Empty configuration value for logging')
        if isinstance(val, Logging):
            return val
        if isinstance(val, bool):
            return Logging.ENABLED if val else Logging.DISABLED
        if val.lower() == 'enabled' or val.lower() in util.TRUE_OPTIONS:
            return Logging.ENABLED
        if val.lower() == 'disabled' or val.lower() in util.FALSE_OPTIONS:
            return Logging.DISABLED
        if val.lower() == 'propagate':
            return Logging.PROPAGATE
        raise ValueError('Unknown configuration value for logging: ' + val)


# ------------ DEFAULT VALUES ------------ #
DEF_LOG = Logging.DISABLED
DEF_LOG_STDOUT_LEVEL = 'off'
DEF_LOG_FILE_LEVEL = 'off'
DEF_LOG_FILE_PATH = None

DEF_PERSISTENCE_ENABLED = False
DEF_PERSISTENCE_TYPE = 'sqlite'
DEF_PERSISTENCE_DATABASE = ''

DEF_PLUGINS = ()
DEF_ACTION = '--help'

# ------------ CONFIG VALUES ------------ #

log = DEF_LOG
log_stdout_level = DEF_LOG_STDOUT_LEVEL
log_file_level = DEF_LOG_FILE_LEVEL
log_file_path = DEF_LOG_FILE_PATH

persistence_enabled = DEF_PERSISTENCE_ENABLED
persistence_type = DEF_PERSISTENCE_TYPE
persistence_database = DEF_PERSISTENCE_DATABASE

plugins = DEF_PLUGINS
default_action = DEF_ACTION


def set_variables(**kwargs):
    module = sys.modules[__name__]
    for name, value in kwargs.items():
        if name == 'log_enabled':
            name = 'log'  # `log_enabled` is alias for `log` as this name is used in config file
        cur_value = getattr(module, name)
        if type(value) == type(cur_value):
            value_to_set = value
        elif isinstance(cur_value, bool):  # First bool than int, as bool is int..
            value_to_set = distutils.util.strtobool(value)
        elif isinstance(cur_value, int):
            value_to_set = int(value)
        elif isinstance(cur_value, Logging):
            value_to_set = Logging.from_value(value)
        else:
            raise ValueError(f'Cannot convert value {value} to {type(cur_value)}')

        setattr(module, name, value_to_set)
