"""
This module defines the 'job' component of the job framework. Together with the 'instance' component
in the `inst` module, they establish the foundation of this framework.

A job consists of an ID and may have static attributes. When a job is executed, it creates a job instance.
Naturally, while the specific task a job instance performs is user-defined, each instance of the same job is expected
to perform the same task.
"""


import datetime
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, Any, List, Optional

from tarotools.taro.jobs.execution import ExecutionState
from tarotools.taro.util import MatchingStrategy, format_dt_iso


class Job:
    """
    Represents a job definition.

    This class encapsulates the properties and characteristics of a job. Each individual execution
    of a job is represented as an object of the `JobInstance` class.

    Attributes:
        _id (str): Unique identifier for the job.
        _properties (Dict[str, str]): Additional properties or metadata associated with the job.
    """

    def __init__(self, job_id: str, properties: Dict[str, str] = None):
        """
        Initialize a new Job object.

        Args:
            job_id (str): Unique identifier for the job.
            properties (Dict[str, str], optional): Additional properties or metadata. Defaults to an empty dictionary.
        """
        self._id = job_id
        self._properties = properties or {}

    @property
    def id(self) -> str:
        """
        Returns the unique identifier of the job.

        Returns:
            str: The job's unique identifier.
        """
        return self._id

    @property
    def properties(self) -> Dict[str, str]:
        """
        Returns the properties or metadata associated with the job.

        Returns:
            Dict[str, str]: Dictionary containing job properties.
        """
        return self._properties

    def __eq__(self, other: object) -> bool:
        """Checks if two Job objects are equal based on their unique ID and properties."""
        if not isinstance(other, Job):
            return False
        return self._id == other._id and self._properties == other._properties

    def __hash__(self) -> int:
        """Returns the hash based on the job's unique ID and properties."""
        return hash((self._id, frozenset(self._properties.items())))


class JobMatchingCriteria:
    """
    Specifies criteria for matching `Job` instances based on job properties and a matching strategy.

    Attributes:
        properties (Dict[str, str], optional): Dictionary of properties to match against.
        property_match_strategy (MatchingStrategy): Strategy function to use for matching property values.
    """

    def __init__(self, *, properties=None, property_match_strategy=MatchingStrategy.EXACT):
        """
        Initializes the JobMatchingCriteria with the given properties and matching strategy.

        Args:
            properties (Dict[str, str], optional):
                Dictionary of properties to match against.
            property_match_strategy (MatchingStrategy):
                Strategy function to use for matching property values. Defaults to an exact match.
        """
        self.properties = properties
        self.property_match_strategy = property_match_strategy

    def matches(self, job) -> bool:
        """
        Determines if the given job matches the set criteria.

        Args:
            job (Job): The job instance to check against the criteria.

        Returns:
            bool: True if the job matches the criteria, otherwise False.
        """
        if not self.properties:
            return True

        for k, v in self.properties.items():
            prop = job.properties.get(k)
            if not prop:
                return False
            if not self.property_match_strategy(prop, v):
                return False

        return True

    def matched(self, jobs) -> List[Job]:
        """
        Returns a list of jobs that match the set criteria.

        Args:
            jobs (List[Job]): A list of job instances to check against the criteria.

        Returns:
            List[Job]: A list of job instances that match the criteria.
        """
        return [job for job in jobs if self.matches(job)]


@dataclass
class JobStats:
    """
    Represents the statistics related to a specific job over a certain time period.

    These statistics provide insights into the job's performance and status trends during
    the specified timeframe.

    Attributes:
        job_id (str): Unique identifier for the job.
        count (int): Number of instances of the job within the time interval.
        first_created (datetime): Creation time of the first job instance in the interval.
        last_created (datetime): Creation time of the last job instance in the interval.
        fastest_time (timedelta): Shortest execution time among all instances in the interval.
        average_time (timedelta): Average execution time across all instances in the interval.
        slowest_time (timedelta): Longest execution time among all instances in the interval.
        last_time (timedelta): Execution time of the most recent instance in the interval.
        last_state (ExecutionState): State of the last executed instance in the interval.
        failed_count (int): Number of instances that failed during the time interval.
        warning_count (int): Number of instances with at least one warning during the time interval.
    """

    job_id: str
    count: int = 0
    first_created: Optional[datetime] = None
    last_created: Optional[datetime] = None
    fastest_time: Optional[timedelta] = None
    average_time: Optional[timedelta] = None
    slowest_time: Optional[timedelta] = None
    last_time: Optional[timedelta] = None
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
