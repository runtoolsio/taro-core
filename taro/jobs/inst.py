"""
Job framework defines components used for job submission and management.
It is built upon :mod:`execution` framework.
It provides constructs for:
  1. Creating of job definition
  2. Implementing of job instance
  3. Implementing of job observer

There are two type of clients of the framework:
  1. Job users
  2. Job management implementation
"""

import abc
import datetime
import textwrap
from collections import namedtuple
from dataclasses import dataclass
from enum import Enum
from fnmatch import fnmatch
from functools import partial
from typing import NamedTuple, Dict, Any, Optional, Callable, Union, List, Tuple, Iterable, Set

from taro.jobs.execution import ExecutionError, ExecutionLifecycle, ExecutionPhase, ExecutionStateFlag
from taro.jobs.track import TrackedTaskInfo
from taro.util import and_, or_, MatchingStrategy, is_empty, to_list, format_dt_iso, remove_empty_values, \
    single_day_range, \
    days_range

DEFAULT_OBSERVER_PRIORITY = 100

S = Union[Callable[[str, str], bool], MatchingStrategy]


@dataclass
class IDMatchingCriteria:
    """
    This class specifies criteria for matching :class:`JobInstanceID` instances.
    If both job_id and instance_id are empty, the matching strategy defaults to :class:`MatchingStrategy.ALWAYS_TRUE`.

    Attributes:
    job_id (str): The pattern for job ID matching. If empty, it is ignored.
    instance_id (str): The pattern for instance ID matching. If empty, it is ignored.
    match_both_ids (bool): If False, a match with either job_id or instance_id is sufficient. Default is True.
    strategy (S): The strategy to use for matching. Default is :class:`MatchingStrategy.EXACT`.
    """
    job_id: str
    instance_id: str  # TODO consider default value too
    match_both_ids: bool = True
    strategy: S = MatchingStrategy.EXACT

    @classmethod
    def none_match(cls):
        return cls('', '', True, MatchingStrategy.ALWAYS_FALSE)

    @classmethod
    def parse_pattern(cls, pattern, strategy: S = MatchingStrategy.EXACT):
        if "@" in pattern:
            job_id, instance_id = pattern.split("@")
            match_both = True
        else:
            job_id = instance_id = pattern
            match_both = False
        return cls(job_id, instance_id, match_both, strategy)

    @classmethod
    def from_dict(cls, as_dict):
        return cls(as_dict['job_id'],
                   as_dict['instance_id'],
                   as_dict['match_both_ids'],
                   MatchingStrategy[as_dict['strategy'].upper()])

    def _op(self):
        return and_ if self.match_both_ids else or_

    def matches(self, jid):
        op = self._op()
        return op(not self.job_id or self.strategy(jid.job_id, self.job_id),
                  not self.instance_id or self.strategy(jid.instance_id, self.instance_id))

    def __call__(self, jid):
        return self.matches(jid)

    def matches_instance(self, job_instance):
        return self.matches(job_instance.id)

    def to_dict(self, include_empty=True):
        d = {
            'job_id': self.job_id,
            'instance_id': self.instance_id,
            'match_both_ids': self.match_both_ids,
            'strategy': self.strategy.name.lower(),
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}


def compound_id_filter(criteria_seq):
    def match(jid):
        return not criteria_seq or any(criteria(jid) for criteria in criteria_seq)

    return match


class LifecycleEvent(Enum):

    CREATED = partial(lambda l: l.created_at)
    EXECUTED = partial(lambda l: l.executed_at)
    ENDED = partial(lambda l: l.ended_at)

    def __call__(self, instance) -> Optional[datetime.datetime]:
        return self.value(instance)

    @classmethod
    def decode(cls, value):
        return cls[value]

    def encode(self):
        return self.name


class IntervalCriteria:

    def __init__(self, event, from_dt=None, to_dt=None, *, include_to=True):
        if not event:
            raise ValueError('Interval criteria event must be provided')
        if not from_dt and not to_dt:
            raise ValueError('Interval cannot be empty')

        self._event = event
        self._from_dt = from_dt
        self._to_dt = to_dt
        self._include_to = include_to

    @classmethod
    def from_dict(cls, data):
        event = LifecycleEvent.decode(data['event'])
        from_dt = data.get("from_dt", None)
        to_dt = data.get("to_dt", '')
        include_to = data['include_to']
        return cls(event, from_dt, to_dt, include_to=include_to)

    @classmethod
    def single_day_period(cls, event, day, *, local_tz=False):
        range_ = single_day_range(day, local_tz=local_tz)
        return cls(event, *range_, include_to=False)

    @classmethod
    def today(cls, event, *, local_tz=False):
        return cls.single_day_period(event, 0, local_tz=local_tz)

    @classmethod
    def yesterday(cls, event, *, local_tz=False):
        return cls.single_day_period(event, -1, local_tz=local_tz)

    @classmethod
    def days_interval(cls, event, days, *, local_tz=False):
        range_ = days_range(days, local_tz=local_tz)
        return cls(event, *range_, include_to=False)

    @classmethod
    def week_back(cls, event, *, local_tz=False):
        return cls.days_interval(event, -7, local_tz=local_tz)

    @property
    def event(self):
        return self._event

    @property
    def from_dt(self):
        return self._from_dt

    @property
    def to_dt(self):
        return self._to_dt

    @property
    def include_to(self):
        return self._include_to

    def __call__(self, lifecycle):
        return self.matches(lifecycle)

    def matches(self, lifecycle):
        event_dt = self.event(lifecycle)
        if not event_dt:
            return False
        if self.from_dt and event_dt < self.from_dt:
            return False
        if self.to_dt:
            if self.include_to:
                if event_dt > self.to_dt:
                    return False
            else:
                if event_dt >= self.to_dt:
                    return False

        return True

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        d = {
            "event": self.event.encode(),
            "from_dt": format_dt_iso(self.from_dt),
            "to_dt": format_dt_iso(self.to_dt),
            "include_to": self.include_to,
        }
        return remove_empty_values(d) if include_empty else d


class StateCriteria:

    def __init__(self, *, flag_groups: Iterable[Set[ExecutionStateFlag]] = (), warning: Optional[bool] = None):
        self._flags = flag_groups
        self._warning = warning

    @classmethod
    def from_dict(cls, data):
        flag_groups = data.get('flag_groups', ())
        warning = data.get('warning', None)
        return cls(flag_groups=flag_groups, warning=warning)

    @property
    def flag_groups(self):
        return self._flags

    @property
    def warning(self):
        return self._warning

    def __call__(self, instance):
        return self.matches(instance)

    def matches(self, instance):
        if self.flag_groups and \
                not any(all(f in instance.lifecycle.state.flags for f in g) for g in self.flag_groups):
            return False

        if self.warning is not None and self.warning != bool(instance.warnings):
            return False

        return True

    def __bool__(self):
        return bool(self.flag_groups) or self.warning is not None

    def to_dict(self, include_empty=True):
        d = {
            "flag_groups": self.flag_groups,
            "warning": self.warning,
        }
        return remove_empty_values(d) if include_empty else d


class InstanceMatchingCriteria:

    def __init__(self, id_criteria=None, interval_criteria=None, state_criteria=None):
        self._id_criteria = to_list(id_criteria)
        self._interval_criteria = to_list(interval_criteria)
        self._state_criteria = state_criteria

    @classmethod
    def parse_pattern(cls, pattern, strategy: S = MatchingStrategy.EXACT):
        return cls(IDMatchingCriteria.parse_pattern(pattern, strategy))

    @classmethod
    def from_dict(cls, as_dict):
        id_criteria = [IDMatchingCriteria.from_dict(c) for c in as_dict.get('id_criteria', ())]
        interval_criteria = [IntervalCriteria.from_dict(c) for c in as_dict.get('interval_criteria', ())]
        sc = as_dict.get('state_criteria')
        state_criteria = StateCriteria.from_dict(sc) if sc else None
        return cls(id_criteria, interval_criteria, state_criteria)

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

    def matches_id(self, job_instance):
        return not self.id_criteria or compound_id_filter(self.id_criteria)(job_instance)

    def matches_interval(self, job_instance):
        return not self.interval_criteria or any(c(job_instance.lifecycle) for c in self.interval_criteria)

    def matches_state(self, job_instance):
        return self.state_criteria is None or self.state_criteria(job_instance)

    def matches(self, job_instance):
        return self.matches_id(job_instance) and self.matches_interval(job_instance) \
            and self.matches_state(job_instance)

    def to_dict(self, include_empty=True):
        d = {
            'id_criteria': [c.to_dict(include_empty) for c in self.id_criteria],
            'interval_criteria': [c.to_dict(include_empty) for c in self.interval_criteria],
            'state_criteria': self.state_criteria.to_dict(include_empty) if self.state_criteria else None
        }
        return remove_empty_values(d) if include_empty else d

    def __bool__(self):
        return bool(self.id_criteria) or bool(self.interval_criteria) or bool(self.state_criteria)

    def __repr__(self):
        return f"{self.__class__.__name__}(" \
               f"id_criteria={self._id_criteria}," \
               f"interval_criteria={self._interval_criteria}, " \
               f"state_criteria={self.state_criteria})"


def parse_criteria(pattern, strategy: S = MatchingStrategy.EXACT):
    return InstanceMatchingCriteria.parse_pattern(pattern, strategy)


class JobInstanceID(NamedTuple):
    """
    TODO Create a method returning a no-match of this ID
    """
    job_id: str
    instance_id: str

    @classmethod
    def from_dict(cls, as_dict):
        return cls(as_dict['job_id'], as_dict['instance_id'])

    def matches_pattern(self, id_pattern, matching_strategy=fnmatch):
        return IDMatchingCriteria.parse_pattern(id_pattern, strategy=matching_strategy).matches(self)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "instance_id": self.instance_id
        }

    def __eq__(self, other):
        if type(self) is type(other):
            return self.job_id == other.job_id and self.instance_id == other.instance_id
        else:
            return False

    def __hash__(self):
        return hash((self.job_id, self.instance_id))

    def __repr__(self):
        return "{}@{}".format(self.job_id, self.instance_id)


@dataclass
class JobInstanceMetadata:

    id: JobInstanceID
    parameters: Tuple[Tuple[str, str]]
    user_params: Dict[str, Any]
    pending_group: Optional[str] = None

    @classmethod
    def from_dict(cls, as_dict):
        return cls(JobInstanceID.from_dict(as_dict['id']),
                   as_dict['parameters'],
                   as_dict['user_params'],
                   as_dict['pending_group'])

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        d = {
            "id": self.id.to_dict(),
            "parameters": self.parameters,
            "user_params": self.user_params,
            "pending_group": self.pending_group,
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"job_id={self.id!r}, "
            f"parameters={self.parameters!r}, "
            f"user_params={self.user_params!r},"
            f"pending_group={self.pending_group!r})"
        )


class JobInstance(abc.ABC):

    @property
    def job_id(self) -> str:
        """Job part of the instance identifier"""
        return self.id.job_id

    @property
    def instance_id(self) -> str:
        """Instance part of the instance identifier"""
        return self.id.instance_id

    @property
    @abc.abstractmethod
    def id(self):
        """Identifier of this instance"""

    @property
    @abc.abstractmethod
    def metadata(self):
        """Descriptive information of this instance"""

    @abc.abstractmethod
    def run(self):
        """Run the job"""

    @abc.abstractmethod
    def release(self):
        """Release the job if it is waiting to be synchronised otherwise ignore"""

    @property
    @abc.abstractmethod
    def lifecycle(self):
        """Execution lifecycle of this instance"""

    @property
    @abc.abstractmethod
    def tracking(self):
        """Task tracking information, None if tracking is not supported"""

    @property
    @abc.abstractmethod
    def status(self):
        """Current status of the job or None if not supported"""

    @property
    @abc.abstractmethod
    def last_output(self):
        """Last lines of output or None if not supported"""

    @property
    @abc.abstractmethod
    def error_output(self):
        """Lines of error output or None if not supported"""

    @property
    @abc.abstractmethod
    def warnings(self):
        """
        TODO Warning as custom type?
        Return dictionary of {alarm_name: occurrence_count}

        :return: warnings
        """

    @abc.abstractmethod
    def add_warning(self, warning):
        """
        Add warning to the instance

        :param warning: warning to add
        """

    @property
    @abc.abstractmethod
    def exec_error(self) -> ExecutionError:
        """Job execution error if occurred otherwise None"""

    @abc.abstractmethod
    def create_info(self):
        """
        Create consistent (thread-safe) snapshot of job instance state

        :return: job (instance) info
        """

    @abc.abstractmethod
    def stop(self):
        """
        Stop running execution gracefully
        """

    @abc.abstractmethod
    def interrupted(self):
        """
        Notify about keyboard interruption signal
        """

    @abc.abstractmethod
    def add_state_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        """
        TODO Should move to Lifecycle class?
        Register execution state observer
        Observer can be:
            1. An instance of ExecutionStateObserver
            2. Callable object with single parameter of JobInfo type

        :param observer: observer to register
        :param priority: observer priority as number (lower number is notified first)
        """

    @abc.abstractmethod
    def remove_state_observer(self, observer):
        """
        De-register execution state observer

        :param observer observer to de-register
        """

    @abc.abstractmethod
    def add_warning_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        """
        Register warning observer

        :param observer: observer to register
        :param priority: observer priority as number (lower number is notified first)
        """

    @abc.abstractmethod
    def remove_warning_observer(self, observer):
        """
        De-register warning observer

        :param observer observer to de-register
        """

    @abc.abstractmethod
    def add_output_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        """
        Register output observer

        :param observer: observer to register
        :param priority: observer priority as number (lower number is notified first)
        """

    @abc.abstractmethod
    def remove_output_observer(self, observer):
        """
        De-register output observer

        :param observer observer to de-register
        """


class DelegatingJobInstance(JobInstance):

    def __init__(self, delegated):
        self.delegated = delegated

    @property
    def id(self):
        return self.delegated.id

    @property
    def metadata(self):
        return self.metadata

    @abc.abstractmethod
    def run(self):
        """Run the job"""

    @property
    def lifecycle(self):
        return self.delegated.lifecycle

    @property
    def status(self):
        return self.delegated.status

    @property
    def last_output(self):
        return self.delegated.last_output

    @property
    def error_output(self):
        return self.delegated.error_output

    @property
    def warnings(self):
        return self.delegated.warnings

    def add_warning(self, warning):
        self.delegated.add_warning(warning)

    @property
    def exec_error(self) -> ExecutionError:
        return self.delegated.exec_error

    def create_info(self):
        return self.delegated.create_info()

    def stop(self):
        self.delegated.stop()

    def interrupted(self):
        self.delegated.interrupted()

    def add_state_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self.delegated.add_state_observer(observer)

    def remove_state_observer(self, observer):
        self.delegated.remove_state_observer(observer)

    def add_warning_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self.delegated.add_warning_observer(observer)

    def remove_warning_observer(self, observer):
        self.delegated.remove_warning_observer(observer)

    def add_output_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self.delegated.add_output_observer(observer)

    def remove_output_observer(self, observer):
        self.delegated.remove_output_observer(observer)


class JobInfo:
    """
    Immutable snapshot of job instance
    """

    @classmethod
    def from_dict(cls, as_dict):
        if as_dict['tracking']:
            tracking = TrackedTaskInfo.from_dict(as_dict['tracking'])
        else:
            tracking = None

        if as_dict['exec_error']:
            exec_error = ExecutionError.from_dict(as_dict['exec_error'])
        else:
            exec_error = None

        return cls(
            JobInstanceMetadata.from_dict(as_dict['metadata']),
            ExecutionLifecycle.from_dict(as_dict['lifecycle']),
            tracking,
            as_dict['status'],
            as_dict['error_output'],
            as_dict['warnings'],
            exec_error,
        )

    def __init__(self, metadata, lifecycle, tracking, status, error_output, warnings, exec_error):
        self._metadata = metadata
        self._lifecycle = lifecycle
        self._tracking = tracking
        if status:
            self._status = textwrap.shorten(status, 1000, placeholder=".. (truncated)", break_long_words=False)
        else:
            self._status = status
        self._error_output = error_output or ()
        self._warnings = warnings or {}
        self._exec_error = exec_error

    @staticmethod
    def created_at(job_info):
        return job_info.lifecycle.created_at

    @property
    def job_id(self) -> str:
        return self.metadata.id.job_id

    @property
    def instance_id(self) -> str:
        return self.metadata.id.instance_id

    @property
    def id(self):
        return self.metadata.id

    @property
    def metadata(self):
        return self._metadata

    @property
    def lifecycle(self):
        return self._lifecycle

    @property
    def state(self):
        return self._lifecycle.state

    @property
    def tracking(self):
        return self._tracking

    @property
    def status(self):
        return self._status

    @property
    def warnings(self):
        return self._warnings

    @property
    def error_output(self):
        return self._error_output

    @property
    def exec_error(self) -> ExecutionError:
        return self._exec_error

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        d = {
            "metadata": self.metadata.to_dict(include_empty),
            "lifecycle": self.lifecycle.to_dict(include_empty),
            "tracking": self.tracking.to_dict(include_empty) if self.tracking else None,
            "status": self.status,
            "error_output": self.error_output,
            "warnings": self.warnings,
            "exec_error": self.exec_error.to_dict(include_empty) if self.exec_error else None
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    def __eq__(self, other):
        if not isinstance(other, JobInfo):
            return NotImplemented
        return (self.metadata, self._lifecycle, self._tracking, self._status, self._error_output,
                self._warnings, self._exec_error) == \
            (other.metadata, other._lifecycle, other._tracking, other._status, other._error_output,
             other._warnings, other._exec_error) # TODO

    def __hash__(self):
        return hash((self.metadata, self._lifecycle, self._tracking, self._status, self._error_output,
                     tuple(sorted(self._warnings.items())), self._exec_error))  # TODO

    def __repr__(self):
        return f"{self.__class__.__name__}("f"metadata={self.metadata!r}"

class JobInfoList(list):

    def __init__(self, jobs):
        super().__init__(jobs)

    @property
    def job_ids(self) -> List[str]:
        return [j.id.job_id for j in self]

    def filtered_by_phase(self, execution_phase):
        return [j for j in self if j.lifecycle.state.phase is execution_phase]

    @property
    def scheduled(self):
        return self.filtered_by_phase(ExecutionPhase.SCHEDULED)

    @property
    def executing(self):
        return self.filtered_by_phase(ExecutionPhase.EXECUTING)

    @property
    def terminal(self):
        return self.filtered_by_phase(ExecutionPhase.TERMINAL)

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        return {"jobs": [job.to_dict(include_empty=include_empty) for job in self]}


class ExecutionStateObserver(abc.ABC):

    @abc.abstractmethod
    def state_update(self, job_info: JobInfo):
        """This method is called when job instance execution state is changed."""


@dataclass
class Warn:
    name: str
    params: Optional[Dict[str, Any]] = None


WarnEventCtx = namedtuple('WarnEventCtx', 'count')


class WarningObserver(abc.ABC):

    @abc.abstractmethod
    def new_warning(self, job_info: JobInfo, warning: Warn, event_ctx: WarnEventCtx):
        """This method is called when there is a new warning event."""


class JobOutputObserver(abc.ABC):

    @abc.abstractmethod
    def job_output_update(self, job_info: JobInfo, output, is_error):
        """
        Executed when new output line is available.

        :param job_info: job instance producing the output
        :param output: job instance output text
        :param is_error: True when it is an error output
        """


class JobOutputTracker(JobOutputObserver):

    def __init__(self, output_tracker):
        self.output_tracker = output_tracker

    def job_output_update(self, job_info: JobInfo, output, is_error):
        self.output_tracker.new_output(output)