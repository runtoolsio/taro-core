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
import textwrap
from collections import namedtuple
from dataclasses import dataclass
from fnmatch import fnmatch
from typing import NamedTuple, Dict, Any, Optional, Sequence, Callable, Union

from taro import util
from taro.jobs.execution import ExecutionError, ExecutionState, ExecutionLifecycle
from taro.util import and_, or_, MatchingStrategy

DEFAULT_OBSERVER_PRIORITY = 100


class IDMatchingCriteria(NamedTuple):
    patterns: Sequence[str]
    strategy: Union[Callable[[str, str], bool], MatchingStrategy] = MatchingStrategy.EXACT

    def __bool__(self):
        return bool(self.patterns) and bool(self.strategy)


class InstanceMatchingCriteria:

    def __init__(self, id_matching_criteria):
        self.id_matching_criteria = id_matching_criteria


class JobInstanceID(NamedTuple):
    job_id: str
    instance_id: str

    def matches_any(self, matching_criteria):
        return any(self.matches(pattern, matching_criteria.strategy) for pattern in matching_criteria.patterns)

    def matches(self, id_pattern, matching_strategy=fnmatch):
        if not id_pattern:
            return False

        if "@" in id_pattern:
            job_id, instance_id = id_pattern.split("@")
            op = and_
        else:
            job_id = instance_id = id_pattern
            op = or_
        return op(not job_id or matching_strategy(self.job_id, job_id),
                  not instance_id or matching_strategy(self.instance_id, instance_id))

    def __eq__(self, other):
        if type(self) is type(other):
            return self.job_id == other.job_id and self.instance_id == other.instance_id
        else:
            return False

    def __hash__(self):
        return hash((self.job_id, self.instance_id))

    def __repr__(self):
        return "{}@{}".format(self.job_id, self.instance_id)


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

    @abc.abstractmethod
    def run(self):
        """Run the job"""

    @abc.abstractmethod
    def release(self, pending_group) -> bool:
        """Release the job if it is waiting in the pending group otherwise ignore"""

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

        :param warning warning to add
        """

    @property
    @abc.abstractmethod
    def exec_error(self) -> ExecutionError:
        """Job execution error if occurred otherwise None"""

    @property
    @abc.abstractmethod
    def parameters(self):
        """List of job parameters"""

    @property
    @abc.abstractmethod
    def user_params(self):
        """Dictionary of arbitrary use parameters"""

    @abc.abstractmethod
    def create_info(self):
        """
        Create consistent (thread-safe) snapshot of job instance state

        :return job (instance) info
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

    @property
    def parameters(self):
        return self.delegated.parameters

    @property
    def user_params(self):
        return self.delegated.user_params

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
        state_changes = ((ExecutionState[state_change['state']], util.str_to_datetime(state_change['changed']))
                         for state_change in as_dict['lifecycle']['state_changes'])
        lifecycle = ExecutionLifecycle(*state_changes)

        if as_dict['exec_error']:
            exec_error = ExecutionError(as_dict['exec_error']['message'],
                                        ExecutionState[as_dict['exec_error']['state']])
        else:
            exec_error = None

        return cls(
            JobInstanceID(as_dict['id']['job_id'], as_dict['id']['instance_id']),
            lifecycle,
            None,  # TODO
            as_dict['status'],
            as_dict['error_output'],
            as_dict['warnings'],
            exec_error,
            as_dict['parameters'],
            **as_dict['user_params']
        )

    def __init__(self, job_instance_id, lifecycle, tracking, status, error_output, warnings, exec_error: ExecutionError,
                 parameters, **user_params):
        self._job_instance_id = job_instance_id
        self._lifecycle = lifecycle
        self._tracking = tracking
        if status:
            self._status = textwrap.shorten(status, 1000, placeholder=".. (truncated)", break_long_words=False)
        else:
            self._status = status
        self._error_output = tuple(error_output)
        self._warnings = warnings
        self._exec_error = exec_error
        self._parameters = tuple(parameters)
        self._user_params = user_params

    @staticmethod
    def created(job_info):
        return job_info.lifecycle.changed(ExecutionState.CREATED)

    @property
    def job_id(self) -> str:
        return self._job_instance_id.job_id

    @property
    def instance_id(self) -> str:
        return self._job_instance_id.instance_id

    @property
    def id(self):
        return self._job_instance_id

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

    @property
    def parameters(self):
        return self._parameters

    @property
    def user_params(self):
        return dict(self._user_params)

    def matches(self, instance_matching_criteria):
        if not instance_matching_criteria:
            return ValueError('No instance matching criteria')
        if not instance_matching_criteria.id_matching_criteria:
            return ValueError('ID matching criteria must be set in instance matching criteria')

        return self.id.matches_any(instance_matching_criteria.id_matching_criteria)

    def to_dict(self) -> Dict[str, Any]:
        lc = self.lifecycle
        state_changes = [{"state": state.name, "changed": datetime_str(change)} for state, change in lc.state_changes]
        if self.exec_error:
            exec_error = {"message": self.exec_error.message, "state": self.exec_error.exec_state.name}
        else:
            exec_error = None

        return {
            "id": {
                "job_id": self.job_id,
                "instance_id": self.instance_id,
            },
            "lifecycle": {
                "state_changes": state_changes,
                "state": lc.state.name,
                "created": datetime_str(lc.changed(ExecutionState.CREATED)),
                "last_changed": datetime_str(lc.last_changed),
                "execution_started": datetime_str(lc.execution_started),
                "execution_finished": datetime_str(lc.execution_finished),
                "execution_time": lc.execution_time.total_seconds() if lc.execution_started else None,
            },
            "status": self.status,
            "error_output": self.error_output,
            "warnings": self.warnings,
            "exec_error": exec_error,
            "parameters": self.parameters,
            "user_params": self.user_params
        }

    def __repr__(self) -> str:
        return "{}({!r}, {!r}, {!r}, {!r}, {!r})".format(
            self.__class__.__name__, self._job_instance_id, self._lifecycle, self._status, self._warnings,
            self._exec_error)


def datetime_str(td):
    if td is None:
        return None
    return td.isoformat()


class JobInfoCollection:

    def __init__(self, *jobs):
        self._jobs = jobs

    @property
    def jobs(self):
        return list(self._jobs)


class Job:

    def __init__(self, job_id, properties: dict):
        self._job_id = job_id
        self._properties = properties

    @property
    def job_id(self):
        return self._job_id

    @property
    def properties(self):
        return self._properties


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
