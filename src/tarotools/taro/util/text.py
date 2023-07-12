import re
from enum import Enum
from fnmatch import fnmatch
from operator import eq
from typing import Dict


def split_params(params, kv_sep="=") -> Dict[str, str]:
    f"""
    Converts sequence of values in format "key{kv_sep}value" to dict[key, value]
    """
    if not params:
        return {}

    def split(s):
        if len(s) < 3 or kv_sep not in s[1:-1]:
            raise ValueError(f"Parameter must be in format: param{kv_sep}value")
        return s.split(kv_sep)

    return {k: v for k, v in (split(set_opt) for set_opt in params)}


def truncate(text, max_len, truncated_suffix=''):
    text_length = len(text)
    suffix_length = len(truncated_suffix)

    if suffix_length > max_len:
        raise ValueError(f"Truncated suffix length {suffix_length} is larger than max length {max_len}")

    if text_length > max_len:
        return text[:(max_len - suffix_length)] + truncated_suffix

    return text


def partial_match(string, pattern):
    return bool(re.search(pattern, string))

def always_true(*_):
    return True

def always_false(*_):
    return False


class MatchingStrategy(Enum):
    """
    Define functions for string match testing where the first parameter is the tested string and the second parameter
    is the pattern.
    """
    EXACT = (eq,)
    FN_MATCH = (fnmatch,)
    PARTIAL = (partial_match,)
    ALWAYS_TRUE = (always_true,)
    ALWAYS_FALSE = (always_false,)

    def __call__(self, *args, **kwargs):
        return self.value[0](*args, **kwargs)


def convert_if_number(val):
    if isinstance(val, (int, float)) or not val:
        return val

    if '.' in (dec := val.replace(',', '.')):
        try:
            return float(dec)
        except ValueError:
            pass

    try:
        return int(val)
    except ValueError:
        pass

    return val
