import functools
import os
import secrets
from datetime import datetime
from types import SimpleNamespace


class NestedNamespace(SimpleNamespace):

    def get(self, fields: str, default=None, type_=None):
        return get_attr(self, fields, default, type_)


# Martijn Pieters' solution below: https://stackoverflow.com/questions/50490856
@functools.singledispatch
def wrap_namespace(ob):
    return ob


@wrap_namespace.register(dict)
def _wrap_dict(ob):
    return NestedNamespace(**{k: wrap_namespace(v) for k, v in ob.items()})


@wrap_namespace.register(list)
def _wrap_list(ob):
    return [wrap_namespace(v) for v in ob]


def get_attr(obj, fields, default=None, type_=None):
    return _getattr(obj, fields.split('.'), default, type_)


def _getattr(obj, fields, default, type_):
    attr = getattr(obj, fields[0], default)

    if attr is None:
        return default

    if len(fields) == 1:
        if type_ and not isinstance(attr, type_):
            return default
        return attr
    else:
        return _getattr(attr, fields[1:], default, type_)


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

    # Workaround: https://stackoverflow.com/questions/30999230/how-to-parse-timezone-with-colon to support Python <3.7
    if ":" == str_ts[-3:-2]:
        str_ts = str_ts[:-3] + str_ts[-2:]

    return datetime.strptime(str_ts, "%Y-%m-%d" + sep + "%H:%M:%S.%f%z")


def format_timedelta(td):
    mm, ss = divmod(td.seconds, 60)
    hh, mm = divmod(mm, 60)
    s = "%02d:%02d:%02d" % (hh, mm, ss)
    if td.days:
        def plural(n):
            return n, abs(n) != 1 and "s" or ""

        s = ("%d day%s, " % plural(td.days)) + s
    if td.microseconds:
        s = s + ".%06d" % td.microseconds
        # s = s + ("%f" % (td.microseconds / 1000000))[1:-3]
    return s


def expand_user(file):
    if file is None or not file.startswith('~'):
        return file

    return os.path.expanduser(file)
