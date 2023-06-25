import datetime
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, Any

from taro.jobs.execution import ExecutionState
from taro.util import MatchingStrategy


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
        return {'job_id': self.job_id, 'count': self.count}
