"""Global configuration

Implementation of config pattern:
https://docs.python.org/3/faq/programming.html#how-do-i-share-global-variables-across-modules
"""

import distutils.util
import sys
from enum import Enum, auto

from tarotools.taro import util
from tarotools.taro.util.attr import get_module_attributes


class LogMode(Enum):
    ENABLED = auto()
    PROPAGATE = auto()
    DISABLED = auto()

    @staticmethod
    def from_value(val):
        if val is None:
            raise ValueError('Empty configuration value for log mode')
        if isinstance(val, LogMode):
            return val
        if isinstance(val, bool):
            return LogMode.ENABLED if val else LogMode.DISABLED
        if val.lower() == 'enabled' or val.lower() in util.TRUE_OPTIONS:
            return LogMode.ENABLED
        if val.lower() == 'disabled' or val.lower() in util.FALSE_OPTIONS:
            return LogMode.DISABLED
        if val.lower() == 'propagate':
            return LogMode.PROPAGATE
        raise ValueError('Unknown configuration value for logging: ' + val)


# ------------ DEFAULT VALUES ------------ #
DEF_LOG = LogMode.DISABLED
DEF_LOG_STDOUT_LEVEL = 'off'
DEF_LOG_FILE_LEVEL = 'off'
DEF_LOG_FILE_PATH = None
DEF_LOG_TIMING = False

DEF_PERSISTENCE_ENABLED = False
DEF_PERSISTENCE_TYPE = 'sqlite'
DEF_PERSISTENCE_MAX_AGE = ''
DEF_PERSISTENCE_MAX_RECORDS = -1
DEF_PERSISTENCE_DATABASE = ''

DEF_LOCK_TIMEOUT = 10000
DEF_LOCK_MAX_CHECK_TIME = 50

DEF_PLUGINS = ()

# ------------ CONFIG VALUES ------------ #
# !! UPDATE CONFIG.md when changes are made !! #

log_mode = DEF_LOG
log_stdout_level = DEF_LOG_STDOUT_LEVEL
log_file_level = DEF_LOG_FILE_LEVEL
log_file_path = DEF_LOG_FILE_PATH
log_timing = DEF_LOG_TIMING

persistence_enabled = DEF_PERSISTENCE_ENABLED
persistence_type = DEF_PERSISTENCE_TYPE
persistence_max_age = DEF_PERSISTENCE_MAX_AGE
persistence_max_records = DEF_PERSISTENCE_MAX_RECORDS
persistence_database = DEF_PERSISTENCE_DATABASE

lock_timeout_ms = DEF_LOCK_TIMEOUT
lock_max_check_time_ms = DEF_LOCK_MAX_CHECK_TIME

plugins = DEF_PLUGINS


def set_variables(**kwargs):
    module = sys.modules[__name__]
    for name, value in kwargs.items():
        cur_value = getattr(module, name)
        if type(value) == type(cur_value):
            value_to_set = value
        elif isinstance(cur_value, LogMode):  # Must be before bool or str as these types are supported by LogMode parse
            value_to_set = LogMode.from_value(value)
        elif isinstance(cur_value, bool):  # First bool than int, as bool is int..
            value_to_set = distutils.util.strtobool(value)
        elif isinstance(cur_value, int):
            value_to_set = int(value)
        elif isinstance(cur_value, tuple):
            value_to_set = tuple(value)
        else:
            raise TypeError(f'Cannot convert value {value} to {type(cur_value)}')

        setattr(module, name, value_to_set)


def set_nested_ns(nested_ns):
    new_attrs = {}

    current_attrs = get_module_attributes(sys.modules[__name__])
    for attr_name, attr_val in current_attrs.items():
        new_val = nested_ns.get(attr_name, sep="_")
        if new_val is not None:
            new_attrs[attr_name] = new_val

    set_variables(**new_attrs)
