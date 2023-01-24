from enum import Enum
from typing import Optional, List

from taro import util
from taro.jobs.job import IDMatchingCriteria, InstanceMatchingCriteria
from taro.util import DateTimeFormat


def id_matching_criteria(args, def_id_match_strategy) -> Optional[IDMatchingCriteria]:
    """
    :param args: cli args
    :param def_id_match_strategy: id match strategy used when not overridden by args
    :return: instance of ID match criteria or None when args has no criteria specified
    """
    if args.instances:
        return IDMatchingCriteria(args.instances, def_id_match_strategy)
    else:
        return None


def instance_matching_criteria(args, def_id_match_strategy) -> Optional[InstanceMatchingCriteria]:
    if args.instances:
        return InstanceMatchingCriteria(IDMatchingCriteria(args.instances, def_id_match_strategy))
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

