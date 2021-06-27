import functools
import itertools
import os
import secrets
from datetime import datetime, timezone
from pathlib import Path
from shutil import copy
from types import SimpleNamespace
from typing import Dict

import yaml


class NestedNamespace(SimpleNamespace):

    def get(self, fields: str, default=None, type_=None):
        """Similar to `getattr` but supports chained dot notation for safely accessing nested fields.

        :param fields: field names separated by dot
        :param default: value returned if (possible nested) attribute is not found or is `None`
        :param type_: expected type of the attribute value, an exception is raised if does not match
        :returns: a value of the attribute
        """
        return get_attr(self, fields, default, type_)


# Martijn Pieters' solution below: https://stackoverflow.com/questions/50490856
@functools.singledispatch
def wrap_namespace(ob) -> NestedNamespace:
    """Converts provided dictionary and all dictionaries in its value trees to nested namespace.

    This allows to access nested fields using chained dot notation: value = ns.top.nested
    """
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
        if attr is not None and type_ and not isinstance(attr, type_):
            raise TypeError(f"{attr} is not instance of {type_}")
        return attr
    else:
        return _getattr(attr, fields[1:], default, type_)


def set_attr(obj, fields, value):
    if len(fields) == 1:
        setattr(obj, fields[0], value)
    else:
        set_attr(getattr(obj, fields[0]), fields[1:], value)


def split_params(params, kv_sep="=") -> Dict[str, str]:
    f"""
    Converts sequence of values in format "key{kv_sep}value" to dict[key, value]
    """

    def split(s):
        if len(s) < 3 or kv_sep not in s[1:-1]:
            raise ValueError(f"Parameter must be in format: param{kv_sep}value")
        return s.split(kv_sep)

    return {k: v for k, v in (split(set_opt) for set_opt in params)}


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


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def dt_from_utc_str(str_ts, is_iso=True):
    if not str_ts:
        return None
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


def sequence_view(seq, *, sort_key, asc, limit):
    sorted_seq = sorted(seq, key=sort_key, reverse=not asc)
    return itertools.islice(sorted_seq, 0, limit if limit > 0 else None)


def expand_user(file):
    if not isinstance(file, str) or not file.startswith('~'):
        return file

    return os.path.expanduser(file)


def print_file(path):
    path = expand_user(path)
    print('Showing file: ' + str(path))
    with open(path, 'r') as file:
        print(file.read())


def read_yaml_file(file_path) -> NestedNamespace:
    with open(file_path, 'r') as file:
        config_ns = wrap_namespace(yaml.safe_load(file))
        if config_ns:
            return config_ns
        else:  # File is empty
            return NestedNamespace()


def copy_resource(src: Path, dst: Path, overwrite=False):
    if not dst.parent.is_dir():
        os.makedirs(dst.parent)

    if not dst.exists() or overwrite:
        print("copying file to " + str(dst))
        copy(src, dst)
        print("done!")
        return

    raise FileExistsError('File already exists: ' + str(dst))
