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


def unique_timestamp_hex(random_suffix_length=4):
    return secrets.token_hex(random_suffix_length) + format(int(datetime.utcnow().timestamp() * 1000000), 'x')[::-1]


def dt_from_utc_str(str_ts, is_iso=True):
    sep = "T" if is_iso else " "
    return datetime.strptime(str_ts, "%Y-%m-%d" + sep + "%H:%M:%S.%f%z")
