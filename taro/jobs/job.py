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
from fnmatch import fnmatch
from typing import NamedTuple

from taro.jobs.execution import ExecutionError


class JobInstanceID(NamedTuple):
    job_id: str
    instance_id: str

    def __repr__(self):
        return "{}@{}".format(self.job_id, self.instance_id)


class JobInstance(abc.ABC):

    @property
    def job_id(self) -> str:
        """Identifier of the job of this instance"""
        return self.id.job_id

    @property
    def instance_id(self) -> str:
        """Instance identifier"""
        return self.id.instance_id

    @property
    @abc.abstractmethod
    def id(self):
        """Identifier of this instance"""

    @abc.abstractmethod
    def run(self):
        """Run the job"""

    @property
    @abc.abstractmethod
    def lifecycle(self):
        """Execution lifecycle of this instance"""

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
    def warnings(self):
        """
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
    def interrupt(self):
        """
        Stop running execution immediately
        """

    @abc.abstractmethod
    def add_state_observer(self, observer):
        """
        Register execution state observer
        Observer can be:
            1. An instance of ExecutionStateObserver
            2. Callable object with single parameter of JobInfo type

        :param observer observer to register
        """

    @abc.abstractmethod
    def remove_state_observer(self, observer):
        """
        De-register execution state observer

        :param observer observer to de-register
        """

    @abc.abstractmethod
    def add_warning_observer(self, observer):
        """
        Register warning observer

        :param observer observer to register
        """

    @abc.abstractmethod
    def remove_warning_observer(self, observer):
        """
        De-register warning observer

        :param observer observer to de-register
        """

    @abc.abstractmethod
    def add_output_observer(self, observer):
        """
        Register output observer

        :param observer observer to register
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

    def interrupt(self):
        self.delegated.interrupt()

    def add_state_observer(self, observer):
        self.delegated.add_state_observer(observer)

    def remove_state_observer(self, observer):
        self.delegated.remove_state_observer(observer)

    def add_warning_observer(self, observer):
        self.delegated.add_warning_observer(observer)

    def remove_warning_observer(self, observer):
        self.delegated.remove_warning_observer(observer)

    def add_output_observer(self, observer):
        self.delegated.add_output_observer(observer)

    def remove_output_observer(self, observer):
        self.delegated.remove_output_observer(observer)


class JobInfo:
    """
    Immutable snapshot of job instance state
    """

    def __init__(self, job_instance_id, lifecycle, status, warnings, exec_error: ExecutionError, **params):
        self._job_instance_id = job_instance_id
        self._params = params
        self._lifecycle = lifecycle
        if status:
            self._status = textwrap.shorten(status, 1000, placeholder=".. (truncated)", break_long_words=False)
        else:
            self._status = status
        self._warnings = warnings
        self._exec_error = exec_error

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
    def params(self):
        return dict(self._params)

    @property
    def lifecycle(self):
        return self._lifecycle

    @property
    def state(self):
        return self._lifecycle.state()

    @property
    def status(self):
        return self._status

    @property
    def warnings(self):
        return self._warnings

    @property
    def exec_error(self) -> ExecutionError:
        return self._exec_error

    def matches(self, job_instance, job_matching_strategy=fnmatch):
        return job_matching_strategy(self.job_id, job_instance) or fnmatch(self.instance_id, job_instance)

    def __repr__(self) -> str:
        return "{}({!r}, {!r}, {!r}, {!r}, {!r})".format(
            self.__class__.__name__, self._job_instance_id, self._lifecycle, self._status, self._warnings,
            self._exec_error)


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


Warn = namedtuple('Warn', 'name params')
WarnEventCtx = namedtuple('WarnEventCtx', 'count')


class WarningObserver(abc.ABC):

    @abc.abstractmethod
    def new_warning(self, job_info: JobInfo, warning: Warn, event_ctx: WarnEventCtx):
        """This method is called when there is a new warning event."""


class JobOutputObserver(abc.ABC):

    @abc.abstractmethod
    def output_update(self, job_info: JobInfo, output):
        """Executed when new output line is available."""
