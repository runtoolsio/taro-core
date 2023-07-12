import datetime
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, Any

from tarotools.taro.jobs.execution import ExecutionState
from tarotools.taro.util import MatchingStrategy, format_dt_iso


class Job:

    def __init__(self, job_id, properties: Dict[str, str] = None):
        self._id = job_id
        self._properties = properties or {}

    @property
    def id(self):
        return self._id

    @property
    def properties(self):
        return self._properties


class JobMatchingCriteria:

    def __init__(self, *, properties=None, property_match_strategy=MatchingStrategy.EXACT):
        self.properties = properties
        self.property_match_strategy = property_match_strategy

    def matches(self, job):
        if not self.properties:
            return True

        for k, v in self.properties.items():
            prop = job.properties.get(k)
            if not prop:
                return False
            if not self.property_match_strategy(prop, v):
                return False

        return True

    def matched(self, jobs):
        return [job for job in jobs if self.matches(job)]


@dataclass
class JobStats:
    job_id: str
    count: int = 0
    first_created: datetime = None
    last_created: datetime = None
    fastest_time: timedelta = None
    average_time: timedelta = None
    slowest_time: timedelta = None
    last_time: timedelta = None
    last_state: ExecutionState = ExecutionState.NONE
    failed_count: int = 0
    warning_count: int = 0

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        result = {
            'job_id': self.job_id,
            'count': self.count,
            'last_state': self.last_state.name,
            'failed_count': self.failed_count,
            'warning_count': self.warning_count,
        }

        if self.first_created:
            result['first_created'] = format_dt_iso(self.first_created)
        else:
            result['first_created'] = None

        if self.last_created:
            result['last_created'] = format_dt_iso(self.last_created)
        else:
            result['last_created'] = None

        if self.fastest_time:
            result['fastest_time'] = self.fastest_time.total_seconds()
        else:
            result['fastest_time'] = None

        if self.average_time:
            result['average_time'] = self.average_time.total_seconds()
        else:
            result['average_time'] = None

        if self.slowest_time:
            result['slowest_time'] = self.slowest_time.total_seconds()
        else:
            result['slowest_time'] = None

        if self.last_time:
            result['last_time'] = self.last_time.total_seconds()
        else:
            result['last_time'] = None

        if not include_empty:
            result = {k: v for k, v in result.items() if v is not None}
        return result
