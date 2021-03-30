from enum import Enum

from taro.execution import ExecutionState


class SortCriteria(Enum):
    CREATED = 1
    FINISHED = 2
    TIME = 3


def _sort_key(sort: SortCriteria):
    def key(j):
        if sort == SortCriteria.CREATED:
            return j.lifecycle.changed(ExecutionState.CREATED)
        if sort == SortCriteria.FINISHED:
            return j.lifecycle.execution_finished()
        if sort == SortCriteria.TIME:
            return j.lifecycle.execution_time()
        raise ValueError(sort)

    return key
