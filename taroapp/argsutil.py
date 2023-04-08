from enum import Enum
from typing import Optional, List, Callable

from taro import JobInstanceID
from taro.jobs.job import IDMatchingCriteria, InstanceMatchingCriteria, compound_id_filter
from taro.util import DateTimeFormat


def id_matching_criteria(args, def_id_match_strategy) -> List[IDMatchingCriteria]:
    """
    :param args: cli args
    :param def_id_match_strategy: id match strategy used when not overridden by args TODO
    :return: list of ID match criteria or empty when args has no criteria
    """
    if args.instances:
        return [IDMatchingCriteria.parse_pattern(i, def_id_match_strategy) for i in args.instances]
    else:
        return []


def id_match(args, def_id_match_strategy) -> Callable[[JobInstanceID], bool]:
    return compound_id_filter(id_matching_criteria(args, def_id_match_strategy))


def instance_matching_criteria(args, def_id_match_strategy) -> Optional[InstanceMatchingCriteria]:
    if args.instances:
        return InstanceMatchingCriteria(id_matching_criteria(args, def_id_match_strategy))
    else:
        return None


class TimestampFormat(Enum):
    DATE_TIME = DateTimeFormat.DATE_TIME_MS_LOCAL_ZONE
    TIME = DateTimeFormat.TIME_MS_LOCAL_ZONE
    NONE = DateTimeFormat.NONE
    UNKNOWN = None

    def __repr__(self) -> str:
        return self.name.lower().replace("_", "-")

    @staticmethod
    def from_str(string: str) -> "TimestampFormat":
        if not string:
            return TimestampFormat.NONE

        string = string.upper().replace("-", "_")
        try:
            return TimestampFormat[string]
        except KeyError:
            return TimestampFormat.UNKNOWN
