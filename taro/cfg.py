"""
Global configuration
Implementation of config pattern:
https://docs.python.org/3/faq/programming.html#how-do-i-share-global-variables-across-modules
"""

from enum import Enum


class PersistenceType(Enum):
    NONE = 1
    SQL_LITE = 2


log_enabled = False
log_stdout_level = 'off'
log_file_level = 'off'
log_file_path = None

persistence_enabled = False
persistence_type = PersistenceType.NONE
persistence_database = ''

plugins = ()
