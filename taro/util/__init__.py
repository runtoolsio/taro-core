from .containers import *
from .dt import *
from .files import *
from .parser import *
from .text import *

TRUE_OPTIONS = ['yes', 'true', 'y', '1', 'on']
FALSE_OPTIONS = ['no', 'false', 'n', '0', 'off']
BOOLEAN_OPTIONS = TRUE_OPTIONS + FALSE_OPTIONS
LOG_LEVELS = ['critical', 'fatal', 'error', 'warn', 'warning', 'info', 'debug', 'off']


def and_(a, b):
    return a and b


def or_(a, b):
    return a or b
