import functools
import itertools
import os
import re
import secrets
from datetime import datetime, timezone
from enum import Enum
from fnmatch import fnmatch
from operator import eq
from pathlib import Path
from shutil import copy
from typing import Dict

import yaml
from dateutil import relativedelta

from taro import utilns
from taro.utilns import NestedNamespace

TRUE_OPTIONS = ['yes', 'true', 'y', '1', 'on']
FALSE_OPTIONS = ['no', 'false', 'n', '0', 'off']
BOOLEAN_OPTIONS = TRUE_OPTIONS + FALSE_OPTIONS
LOG_LEVELS = ['critical', 'fatal', 'error', 'warn', 'warning', 'info', 'debug', 'off']


def and_(a, b):
    return a and b


def or_(a, b):
    return a or b


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


def str_to_seconds(val):
    value = float(val[:-1])
    unit = val[-1].lower()

    if unit == 's':
        return value
    if unit == 'm':
        return value * 60
    if unit == 'h':
        return value * 60 * 60
    if unit == 'd':
        return value * 60 * 60 * 24

    raise ValueError("Unknown unit: " + unit)


def parse_iso8601_duration(duration):
    match = re.match(r'P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)W)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?', duration)
    if not match:
        raise ValueError('Invalid duration: ' + duration)
    years = int(match.group(1)) if match.group(1) else 0
    months = int(match.group(2)) if match.group(2) else 0
    weeks = int(match.group(3)) if match.group(3) else 0
    days = int(match.group(4)) if match.group(4) else 0
    hours = int(match.group(5)) if match.group(5) else 0
    minutes = int(match.group(6)) if match.group(6) else 0
    seconds = int(match.group(7)) if match.group(7) else 0
    return relativedelta.relativedelta(years=years, months=months, weeks=weeks, days=days, hours=hours, minutes=minutes,
                                       seconds=seconds).normalized()


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
        return utilns.wrap_namespace(yaml.safe_load(file))


def write_yaml_file(content, file_path):
    with open(file_path, 'w') as file:
        return yaml.dump(content, file)


def copy_resource(src: Path, dst: Path, overwrite=False):
    if not dst.parent.is_dir():
        os.makedirs(dst.parent)

    if not dst.exists() or overwrite:
        print("copying file to " + str(dst))
        copy(src, dst)
        print("done!")
        return

    raise FileExistsError('File already exists: ' + str(dst))


def truncate(text, max_len, truncated_suffix=''):
    text_length = len(text)
    suffix_length = len(truncated_suffix)

    if suffix_length > max_len:
        raise ValueError(f"Truncated suffix length {suffix_length} is larger than max length {max_len}")

    if text_length > max_len:
        return text[:(max_len - suffix_length)] + truncated_suffix

    return text


def cli_confirmation():
    print("Do you want to continue? [Y/n] ", end="")
    i = input()
    return i.lower() in TRUE_OPTIONS


def partial_match(string, pattern):
    return bool(re.search(pattern, string))


class MatchingStrategy(Enum):
    """
    Define functions for string match testing where the first parameter is the tested string and the second parameter
    is the pattern.
    """
    EXACT = (eq,)
    FN_MATCH = (fnmatch,)
    PARTIAL = (partial_match,)

    def __call__(self, *args, **kwargs):
        return self.value[0](*args, **kwargs)
