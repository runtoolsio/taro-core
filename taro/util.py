import functools
import secrets
from datetime import datetime


def get_attr(obj, fields, default=None):
    return _getattr(obj, fields.split('.'), default)


def _getattr(obj, fields, default):
    attr = getattr(obj, fields[0], default)

    if attr is None:
        return default

    if len(fields) == 1:
        return attr
    else:
        return _getattr(attr, fields[1:], default)


def set_attr(obj, fields, value):
    if len(fields) == 1:
        setattr(obj, fields[0], value)
    else:
        set_attr(getattr(obj, fields[0]), fields[1:], value)


def prime(func):
    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        next(cr)
        return cr

    return start


def iterates(func):
    @functools.wraps(func)
    def catcher(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except StopIteration:
            pass

    return catcher


def unique_timestamp_hex(random_suffix_length=2):
    return format(int(datetime.utcnow().timestamp() * 1000), 'x') + secrets.token_hex(random_suffix_length)
