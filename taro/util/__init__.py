from .containers import *
from .dt import *
from .files import *
from .parser import *
from .text import *

TRUE_OPTIONS = ('yes', 'true', 'y', '1', 'on')
FALSE_OPTIONS = ('no', 'false', 'n', '0', 'off')
BOOLEAN_OPTIONS = TRUE_OPTIONS + FALSE_OPTIONS
LOG_LEVELS = ('critical', 'fatal', 'error', 'warn', 'warning', 'info', 'debug', 'off')
NUMBER_TYPES = (int, float, complex)


def and_(a, b):
    return a and b


def or_(a, b):
    return a or b


def is_empty(value):
    if value is None:
        return True

    if isinstance(value, NUMBER_TYPES):
        return False

    return not bool(value)


def remove_empty_values(d):
    return {k: v for k, v in d.items() if not is_empty(v)}
