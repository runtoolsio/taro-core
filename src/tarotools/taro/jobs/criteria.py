"""
This module provides various criteria objects used to match job instances or their parts.

TODO: Remove immutable properties
"""

import datetime
from abc import abstractmethod, ABC
from dataclasses import dataclass
from datetime import timezone, time, timedelta
from typing import Dict, Any, Set, Iterable, Optional, TypeVar, Generic

from tarotools.taro import JobRunId
from tarotools.taro.run import TerminationStatusFlag, RunState, Phase, Lifecycle
from tarotools.taro.util import MatchingStrategy, and_, or_, parse, single_day_range, days_range, \
    format_dt_iso, remove_empty_values, to_list

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
class JobRunIdCriterion(MatchCriteria[JobRunId]):
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
    def for_run(job_run):
        """
        Creates an JobRunIdMatchCriteria object that matches the provided job run.

        Args:
            job_run: The specific job run to create a match for.

        Returns:
            JobRunIdCriterion: A criteria object that will match the given job run.
        """
        return JobRunIdCriterion(job_id=job_run.job_id, run_id=job_run.run_id)

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

    def __call__(self, jid):
        return self.matches(jid)

    def matches(self, jid):
        """
        The matching method. It can be also executed by calling this object (`__call__` delegates to this method).

        Args:
            jid: Job run ID to be matched

        Returns:
            bool: Whether the provided job instance ID matches this criteria
        """
        op = self._op()
        return op(not self.job_id or self.strategy(jid.job_id, self.job_id),
                  not self.run_id or self.strategy(jid.run_id, self.run_id))

    def matches_run(self, job_run):
        """
        Args:
            job_run: Job run to be matched

        Returns:
            bool: Whether the provided job run matches this criteria
        """
        return self.matches(job_run.metadata.id)


def compound_id_filter(criteria_seq):
    def match(jid):
        return not criteria_seq or any(criteria(jid) for criteria in criteria_seq)

    return match


@dataclass
class IntervalCriterion(MatchCriteria[Lifecycle]):
    """
    A class to represent criteria for determining if the first occurrence of a given run state in a lifecycle falls
    within a specified datetime interval. This criterion is used to filter or identify lifecycles based
    on the timing of their first transition to the specified run state.

    TODO: Phase

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


class StateCriteria:
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

    def __init__(self, *,
                 phases: Set[Phase] = (),
                 flag_groups: Iterable[Set[TerminationStatusFlag]] = (),
                 warning: Optional[bool] = None):
        self.phases = phases
        self.flag_groups = flag_groups
        self.warning = warning

    @classmethod
    def from_dict(cls, data):
        phases = data.get('phases', ())
        flag_groups = data.get('flag_groups', ())
        warning = data.get('warning', None)
        return cls(phases=phases, flag_groups=flag_groups, warning=warning)

    def __call__(self, instance):
        return self.matches(instance)

    def matches(self, instance):
        if self.phases and instance.lifecycle.phase.phase not in self.phases:
            return False

        if self.flag_groups and \
                not any(all(f in instance.lifecycle.phase.flags for f in g) for g in self.flag_groups):
            return False

        if self.warning is not None and self.warning != bool(instance.warnings):
            return False

        return True

    def __bool__(self):
        return bool(self.phases) or bool(self.flag_groups) or self.warning is not None

    def to_dict(self, include_empty=True):
        d = {
            "phases": self.phases,
            "flag_groups": self.flag_groups,
            "warning": self.warning,
        }
        return remove_empty_values(d) if include_empty else d


class InstanceCriteria:
    """
    This object aggregates various filters, allowing for complex queries and matching against job instances.
    An instance must meet all individual filters for this object to match.

    Properties:
        id_criteria (Optional[Union[JobIDMatchCriteria, Iterable[JobIDMatchCriteria]]]):
            Conditions for matching based on job instance IDs.
            If more than one condition is provided, this filter matches if any of the conditions are met.
        interval_criteria (Optional[Union[IntervalCriteria, Iterable[IntervalCriteria]]]):
            Conditions for matching based on time intervals.
            If more than one condition is provided, this filter matches if any of the conditions are met.
        state_criteria (Optional[StateCriteria]):
            A condition for matching based on job instance state.
        job_ids (Optional[Union[Job, Iterable[Job]]]):
            A job ID or an iterable of IDs for an exact match against specific job instances.
        param_sets (Iterable[Set[Tuple[str, str]]):
            Collection of set of parameters to match. An instance matches if any of the sets matches.
            All tuples in a set must match.
    """

    def __init__(self, id_criteria=None, interval_criteria=None, state_criteria=None, jobs=None, param_sets=None):
        self._id_criteria = to_list(id_criteria)
        self._interval_criteria = to_list(interval_criteria)
        self._state_criteria = state_criteria
        self._job_ids = to_list(jobs)
        self._param_sets = param_sets

    @classmethod
    def parse_pattern(cls, pattern: str, strategy: MatchingStrategy = MatchingStrategy.EXACT):
        return cls(JobRunIdCriterion.parse_pattern(pattern, strategy))

    @classmethod
    def from_dict(cls, as_dict):
        id_criteria = [JobRunIdCriterion.deserialize(c) for c in as_dict.get('id_criteria', ())]
        interval_criteria = [IntervalCriterion.deserialize(c) for c in as_dict.get('interval_criteria', ())]
        sc = as_dict.get('state_criteria')
        state_criteria = StateCriteria.from_dict(sc) if sc else None
        jobs = as_dict.get('jobs', ())
        param_sets = as_dict.get('param_sets', ())
        return cls(id_criteria, interval_criteria, state_criteria, jobs, param_sets)

    @property
    def id_criteria(self):
        return self._id_criteria

    @id_criteria.setter
    def id_criteria(self, criteria):
        self._id_criteria = to_list(criteria)

    @property
    def interval_criteria(self):
        return self._interval_criteria

    @interval_criteria.setter
    def interval_criteria(self, criteria):
        self._interval_criteria = to_list(criteria)

    @property
    def state_criteria(self):
        return self._state_criteria

    @state_criteria.setter
    def state_criteria(self, criteria):
        self._state_criteria = criteria

    @property
    def job_ids(self):
        return self._job_ids

    @job_ids.setter
    def job_ids(self, jobs):
        self._job_ids = to_list(jobs)

    @property
    def param_sets(self):
        return self._param_sets

    @param_sets.setter
    def param_sets(self, param_sets):
        self._param_sets = param_sets

    def matches_id(self, job_instance):
        return not self.id_criteria or compound_id_filter(self.id_criteria)(job_instance)

    def matches_interval(self, job_instance):
        return not self.interval_criteria or any(c(job_instance.lifecycle) for c in self.interval_criteria)

    def matches_state(self, job_instance):
        return self.state_criteria is None or self.state_criteria(job_instance)

    def matches_job_ids(self, job_instance):
        return not self.job_ids or job_instance.job_id in self.job_ids

    def matches_parameters(self, job_instance):
        return (not self.param_sets or
                any(job_instance.metadata.contains_system_parameters(*param_set) for param_set in self.param_sets))

    def __call__(self, job_instance):
        return self.matches(job_instance)

    def matches(self, job_instance):
        """
        Args:
            job_instance (JobInstance): Job instance to match.
        Returns:
            bool: Whether the provided job instance matches all criteria.
        """
        return self.matches_id(job_instance) \
            and self.matches_interval(job_instance) \
            and self.matches_state(job_instance) \
            and self.matches_job_ids(job_instance) \
            and self.matches_parameters(job_instance)

    def to_dict(self, include_empty=True):
        d = {
            'id_criteria': [c.serialize(include_empty) for c in self.id_criteria],
            'interval_criteria': [c.serialize(include_empty) for c in self.interval_criteria],
            'state_criteria': self.state_criteria.serialize(include_empty) if self.state_criteria else None,
            'jobs': self.job_ids,
            'param_sets': self.param_sets,
        }
        return remove_empty_values(d) if include_empty else d

    def __bool__(self):
        return (bool(self.id_criteria) or bool(self.interval_criteria) or bool(self.state_criteria)
                or bool(self.job_ids) or bool(self.param_sets))

    def __repr__(self):
        return f"{self.__class__.__name__}(" \
               f"id_criteria={self._id_criteria}," \
               f"interval_criteria={self._interval_criteria}, " \
               f"state_criteria={self.state_criteria}," \
               f"jobs={self.job_ids}," \
               f"param_sets={self.param_sets})"


def parse_criteria(pattern: str, strategy: MatchingStrategy = MatchingStrategy.EXACT):
    return InstanceCriteria.parse_pattern(pattern, strategy)
