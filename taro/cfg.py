"""Global configuration

Implementation of config pattern:
https://docs.python.org/3/faq/programming.html#how-do-i-share-global-variables-across-modules
"""

import distutils.util
import sys

# ------------ DEFAULT VALUES ------------ #
DEF_LOG_ENABLED = False
DEF_LOG_STDOUT_LEVEL = 'off'
DEF_LOG_FILE_LEVEL = 'off'
DEF_LOG_FILE_PATH = None

DEF_PERSISTENCE_ENABLED = False
DEF_PERSISTENCE_TYPE = 'sqlite'
DEF_PERSISTENCE_DATABASE = ''

DEF_PLUGINS = ()
DEF_ACTION = ''

# ------------ CONFIG VALUES ------------ #

log_enabled = DEF_LOG_ENABLED
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
        cur_value = getattr(module, name)
        if type(value) == type(cur_value):
            value_to_set = value
        elif isinstance(cur_value, bool):  # First bool than int, as bool is int..
            value_to_set = distutils.util.strtobool(value)
        elif isinstance(cur_value, int):
            value_to_set = int(value)
        else:
            raise ValueError(f'Cannot convert value {value} to {type(cur_value)}')

        setattr(module, name, value_to_set)
