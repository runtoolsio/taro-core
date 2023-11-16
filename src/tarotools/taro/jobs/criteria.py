"""
This module provides various criteria objects used to match job instances or their parts.

TODO: Remove immutable properties
"""

import datetime
from abc import abstractmethod, ABC
from dataclasses import dataclass
from datetime import timezone, time, timedelta
from typing import Dict, Any, Set, Iterable, Optional, TypeVar, Generic, Tuple

from tarotools.taro.jobs.instance import JobRun
from tarotools.taro.run import TerminationStatusFlag, RunState, Lifecycle, TerminationInfo
from tarotools.taro.util import MatchingStrategy, and_, or_, parse, single_day_range, days_range, \
    format_dt_iso

T = TypeVar('T')


class MatchCriteria(ABC, Generic[T]):

    @abstractmethod
    def matches(self, tested: T) -> bool:
        """
        Check if the provided tested item matches the criteria.

        :param tested: The item to check against the criteria.
        :return: True if the item matches the criteria, False otherwise.
        """
        pass


@dataclass
class JobRunIdCriterion(MatchCriteria[Tuple[str, str]]):
    """
    This class specifies criteria for matching `JobRun` instances.
    If both `job_id` and `run_id` are empty, the matching strategy defaults to `MatchingStrategy.ALWAYS_TRUE`.

    Attributes:
        job_id (str): The pattern for job ID matching. If empty, the field is ignored.
        run_id (str): The pattern for run ID matching. If empty, the field is ignored.
        match_both_ids (bool): If False, a match with either job_id or instance_id is sufficient. Default is True.
        strategy (MatchingStrategy): The strategy to use for matching. Default is `MatchingStrategy.EXACT`.
    """
    job_id: str
    run_id: str = ''
    match_both_ids: bool = True
    strategy: MatchingStrategy = MatchingStrategy.EXACT

    @classmethod
    def none_match(cls):
        return cls('', '', True, MatchingStrategy.ALWAYS_FALSE)

    @staticmethod
    def for_instance(job_inst):
        """
        Creates an JobRunIdMatchCriteria object that matches the provided job instance.

        Args:
            job_inst: The specific job instance to create a match for.

        Returns:
            JobRunIdCriterion: A criteria object that will match the given job instance.
        """
        return JobRunIdCriterion(job_id=job_inst.metadata.job_id, run_id=job_inst.metadata.run_id)

    @classmethod
    def parse_pattern(cls, pattern: str, strategy=MatchingStrategy.EXACT):
        """
        Parses the provided pattern and returns the corresponding JobRunIdMatchCriteria object.
        The pattern can contain the `@` token to denote `job_id` and `instance_id` parts in this format:
        `{job_id}@{instance_id}`. If the token is not included, then the pattern is matched against both IDs,
        and a match on any fields results in a positive match.

        For matching only `job_id`, use the format: `{job_id}@`
        For matching only `instance_id`, use the format: `@{instance_id}`

        Args:
            pattern (str): The pattern to parse. It can contain a job ID and instance ID separated by '@'.
            strategy (MatchingStrategy, optional): The strategy to use for matching. Default is `MatchingStrategy.EXACT`

        Returns:
            JobRunIdCriterion: A new IDMatchCriteria object with the parsed job_id, instance_id, and strategy.
        """
        if "@" in pattern:
            job_id, instance_id = pattern.split("@")
            match_both = True
        else:
            job_id = instance_id = pattern
            match_both = False
        return cls(job_id, instance_id, match_both, strategy)

    @classmethod
    def deserialize(cls, as_dict):
        return cls(as_dict['job_id'], as_dict['run_id'], as_dict['match_both_ids'],
                   MatchingStrategy[as_dict['strategy'].upper()])

    def serialize(self):
        return {
            'job_id': self.job_id,
            'run_Id': self.run_id,
            'match_both_ids': self.match_both_ids,
            'strategy': self.strategy.name.lower(),
        }

    def _op(self):
        return and_ if self.match_both_ids else or_

    def __call__(self, id_tuple):
        return self.matches(id_tuple)

    def matches(self, id_tuple):
        """
        The matching method. It can be also executed by calling this object (`__call__` delegates to this method).

        Args:
            id_tuple: A tuple of job ID and run ID to be matched

        Returns:
            bool: Whether the provided job instance ID matches this criteria
        """
        op = self._op()
        job_id, run_id = id_tuple
        return op(not self.job_id or self.strategy(job_id, self.job_id),
                  not self.run_id or self.strategy(run_id, self.run_id))

    def matches_instance(self, job_inst):
        """
        Args:
            job_inst: Job instance to be matched

        Returns:
            bool: Whether the provided job instance matches this criteria
        """
        return self.matches((job_inst.metadata.job_id, job_inst.metadata.run_id))


@dataclass
class IntervalCriterion(MatchCriteria[Lifecycle]):
    """
    A class to represent criteria for determining if the first occurrence of a given run state in a lifecycle falls
    within a specified datetime interval. This criterion is used to filter or identify lifecycles based
    on the timing of their first transition to the specified run state.

    Properties:
        run_state (RunState):
            The specific run state whose first occurrence in the lifecycle is to be checked. Default to CREATED state.
        from_dt (datetime, optional):
            The start date-time of the interval. Defaults to None.
        to_dt (datetime, optional):
            The end date-time of the interval. Defaults to None.
        include_to (bool, optional):
            Whether to include the end date-time in the interval. Defaults to True.
    """

    run_state: RunState = RunState.CREATED
    from_dt: Optional[datetime] = None
    to_dt: Optional[datetime] = None
    include_to: bool = True

    @classmethod
    def deserialize(cls, data):
        rs = RunState[data['run_state']]
        from_dt = data.get("from_dt", None)
        to_dt = data.get("to_dt", '')
        include_to = data['include_to']
        return cls(rs, from_dt, to_dt, include_to)

    def serialize(self) -> Dict[str, Any]:
        return {
            "run_state": str(self.run_state),
            "from_dt": format_dt_iso(self.from_dt),
            "to_dt": format_dt_iso(self.to_dt),
            "include_to": self.include_to,
        }

    @classmethod
    def to_utc(cls, event, from_val, to_val):
        """
        Creates criteria with provided values converted to the UTC timezone.

        Args:
            event (LifecycleEvent): The lifecycle event for which the interval is defined.
            from_val (str, datetime, date): The start date-time of the interval.
            to_val (str, datetime, date): The end date-time of the interval.
        """
        if from_val is None and to_val is None:
            raise ValueError('Both `from_val` and `to_val` parameters cannot be None')

        include_to = True

        if from_val is None:
            from_dt = None
        else:
            if isinstance(from_val, str):
                from_val = parse(from_val)
            if isinstance(from_val, datetime.datetime):
                from_dt = from_val.astimezone(timezone.utc)
            else:  # Assuming it is datetime.date
                from_dt = datetime.datetime.combine(from_val, time.min).astimezone(timezone.utc)

        if to_val is None:
            to_dt = None
        else:
            if isinstance(to_val, str):
                to_val = parse(to_val)
            if isinstance(to_val, datetime.datetime):
                to_dt = to_val.astimezone(timezone.utc)
            else:  # Assuming it is datetime.date
                to_dt = datetime.datetime.combine(to_val + timedelta(days=1), time.min).astimezone(timezone.utc)
                include_to = False

        return IntervalCriterion(event, from_dt, to_dt, include_to=include_to)

    @classmethod
    def single_day_period(cls, event, day_offset, *, to_utc=False):
        """
        Creates criteria for a duration of one day.

        Args:
            event (LifecycleEvent): The lifecycle event for which the interval is defined.
            day_offset (int): A day offset for which the period is created. 0 > today, -1 > yesterday, 1 > tomorrow...
            to_utc (bool): The interval is converted from local zone to UTC when set to true.
        """
        range_ = single_day_range(day_offset, to_utc=to_utc)
        return cls(event, *range_, include_to=False)

    @classmethod
    def today(cls, event, *, to_utc=False):
        return cls.single_day_period(event, 0, to_utc=to_utc)

    @classmethod
    def yesterday(cls, event, *, to_utc=False):
        return cls.single_day_period(event, -1, to_utc=to_utc)

    @classmethod
    def days_interval(cls, event, days, *, to_utc=False):
        """
        Creates criteria for an interval extending a specified number of days into the past or future from now.

        Args:
            event (LifecycleEvent):
                The lifecycle event for which the interval is defined.
            days (int):
                Duration of the interval in days. Use a negative number for an interval extending into the past,
                and a positive number for an interval extending into the future.
            to_utc (bool):
                If true, the interval is converted from the local time zone to UTC; otherwise, it remains
                in the local time zone.
        """
        range_ = days_range(days, to_utc=to_utc)
        return cls(event, *range_, include_to=False)

    @classmethod
    def week_back(cls, event, *, to_utc=False):
        return cls.days_interval(event, -7, to_utc=to_utc)

    def __call__(self, lifecycle):
        return self.matches(lifecycle)

    def matches(self, lifecycle):
        checked_dt = lifecycle.state_first_at(self.run_state)
        if not checked_dt:
            return False
        if self.from_dt and checked_dt < self.from_dt:
            return False
        if self.to_dt:
            if self.include_to:
                if checked_dt > self.to_dt:
                    return False
            else:
                if checked_dt >= self.to_dt:
                    return False

        return True


@dataclass
class TerminationCriterion(MatchCriteria[TerminationInfo]):
    """
    This object can be used to filter job instances based on their state, such as instance phase or warnings.
    For the whole criteria to match, all provided filters must be met.

    Properties (filters):
        phases (Set[ExecutionPhase]):
            The criterion match if the instance is in any provided phases.
        flag_groups (Iterable[Set[ExecutionStateFlag]]):
            An iterable of sets, where each set contains flags that define a group. The criterion match if any of
            the provided groups match the state, and a group matches when all flags in the group match the state.
        warning (Optional[bool]):
            A boolean value to filter job instances based on warnings.
            If set to True, only job instances with warnings are matched.
            If set to False, only job instances without warnings are matched.
            If None, the warning status is ignored in the matching.
            Default is None.
    """

    status_flag_groups: Iterable[Set[TerminationStatusFlag]] = ()

    @classmethod
    def deserialize(cls, data):
        status_flag_groups = data.get('status_flag_groups', ())
        return cls(status_flag_groups)

    def serialize(self):
        return {
            "status_flag_groups": self.status_flag_groups,
        }

    def __call__(self, term_info):
        return self.matches(term_info)

    def matches(self, term_info):
        if self.status_flag_groups and \
                not any(all(f in term_info.status.flags for f in g) for g in self.status_flag_groups):
            return False

        return True

    def __bool__(self):
        return bool(self.status_flag_groups)


class JobRunAggregatedCriteria(MatchCriteria[JobRun]):
    """
    This object aggregates various criteria for querying and matching job instances.
    An instance must meet all the provided criteria to be considered a match.

    Properties:
        jobs (List[Job]):
            A list of specific job IDs for matching.
            An instance matches if its job ID is in this list.
        job_run_id_criteria (List[JobRunIdCriterion]):
            A list of criteria for matching based on job run IDs.
            An instance matches if it meets any of the criteria in this list.
        interval_criteria (List[IntervalCriterion]):
            A list of criteria for matching based on time intervals.
            An instance matches if it meets any of the criteria in this list.
        termination_criteria (List[TerminationCriterion]):
            A list of criteria for matching based on termination conditions.
            An instance matches if it meets any of the criteria in this list.

    The class provides methods to check whether a given job instance matches the criteria,
    serialize and deserialize the criteria, and parse criteria from a pattern.
    """

    def __init__(self):
        self.jobs = []
        self.job_run_id_criteria = []
        self.interval_criteria = []
        self.termination_criteria = []

    @classmethod
    def deserialize(cls, as_dict):
        new = cls()
        new.jobs = as_dict.get('jobs', [])
        new.job_run_id_criteria = [JobRunIdCriterion.deserialize(c) for c in as_dict.get('job_run_id_criteria', ())]
        new.interval_criteria = [IntervalCriterion.deserialize(c) for c in as_dict.get('interval_criteria', ())]
        new.termination_criteria = [TerminationCriterion.deserialize(c) for c in as_dict.get('termination_criteria', ())]
        return new

    def serialize(self):
        return {
            'jobs': self.jobs,
            'job_run_id_criteria': [c.serialize() for c in self.job_run_id_criteria],
            'interval_criteria': [c.serialize() for c in self.interval_criteria],
            'state_criteria': [c.serialize() for c in self.termination_criteria],
        }

    @classmethod
    def parse_pattern(cls, pattern: str, strategy: MatchingStrategy = MatchingStrategy.EXACT):
        # TODO
        return cls()

    def __iadd__(self, criterion):
        return self.add(criterion)

    def add(self, criterion):
        match criterion:
            case str():
                self.jobs.append(criterion)
            case JobRunIdCriterion():
                self.job_run_id_criteria.append(criterion)
            case IntervalCriterion():
                self.interval_criteria.append(criterion)
            case TerminationCriterion():
                self.termination_criteria.append(criterion)
            case _:
                raise ValueError("Invalid criterion type")

        return self

    def matches_job_run_id(self, job_run):
        job_id = job_run.metadata.job_id
        run_id = job_run.metadata.run_id
        return not self.job_run_id_criteria or any(c((job_id, run_id)) for c in self.job_run_id_criteria)

    def matches_interval(self, job_run):
        return not self.interval_criteria or any(c(job_run.lifecycle) for c in self.interval_criteria)

    def matches_termination(self, job_run):
        return self.termination_criteria or any(c(job_run.run.termination) for c in self.termination_criteria)

    def matches_jobs(self, job_run):
        return not self.jobs or job_run.job_id in self.jobs

    def __call__(self, job_run):
        return self.matches(job_run)

    def matches(self, job_run):
        """
        Args:
            job_run (JobInstance): Job instance to match.
        Returns:
            bool: Whether the provided job instance matches all criteria.
        """
        return self.matches_job_run_id(job_run) \
            and self.matches_interval(job_run) \
            and self.matches_termination(job_run) \
            and self.matches_jobs(job_run)

    def __bool__(self):
        return (bool(self.job_run_id_criteria)
                or bool(self.interval_criteria)
                or bool(self.termination_criteria)
                or bool(self.jobs))

    def __repr__(self):
        return (f"{self.__class__.__name__}("
                f"{self.job_run_id_criteria=}, "
                f"{self.interval_criteria=}, "
                f"{self.termination_criteria=}, "
                f"{self.jobs=})")


def parse_criteria(pattern: str, strategy: MatchingStrategy = MatchingStrategy.EXACT):
    return JobRunAggregatedCriteria.parse_pattern(pattern, strategy)
