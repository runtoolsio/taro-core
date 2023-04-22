import functools
import itertools
from typing import Iterable


def sequence_view(seq, *, sort_key, asc, limit, filter_=None):
    sorted_seq = sorted(seq, key=sort_key, reverse=not asc)
    if filter_:
        sorted_seq = filter(filter_, sorted_seq)
    return itertools.islice(sorted_seq, 0, limit if limit > 0 else None)


def iterates(func):
    @functools.wraps(func)
    def catcher(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except StopIteration:
            pass

    return catcher


def to_list(e):
    if e is None:
        return []
    return list(e) if isinstance(e, Iterable) else [e]
