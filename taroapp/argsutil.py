from datetime import timedelta, timezone, datetime, time
from enum import Enum
from typing import List, Callable

from taro import JobInstanceID
from taro.jobs.job import IDMatchingCriteria, InstanceMatchingCriteria, compound_id_filter, IntervalCriteria, \
    LifecycleEvent
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


def interval_criteria(args, interval_event=LifecycleEvent.CREATED):
    from_dt = None
    to_dt = None
    include_to = True

    if getattr(args, 'since', None):
        if isinstance(args.since, datetime):
            from_dt = args.since.astimezone(timezone.utc)
        else:  # Assuming it is datetime.date
            from_dt = datetime.combine(args.since, time.min).astimezone(timezone.utc)

    if getattr(args, 'until', None):
        if isinstance(args.until, datetime):
            to_dt = args.until.astimezone(timezone.utc)
        else:  # Assuming it is datetime.date
            to_dt = datetime.combine(args.until + timedelta(days=1), time.min).astimezone(timezone.utc)
            include_to = False

    if from_dt or to_dt:
        return IntervalCriteria(interval_event, from_dt, to_dt, include_to=include_to)
    else:
        return None


def instance_matching_criteria(args, def_id_match_strategy, interval_event=LifecycleEvent.CREATED) ->\
        InstanceMatchingCriteria:
    return InstanceMatchingCriteria(
        id_matching_criteria(args, def_id_match_strategy),
        interval_criteria(args, interval_event))


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
