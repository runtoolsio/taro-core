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

from taro.execution import ExecutionError


class Job:
    def __init__(self, job_id: str, execution, observers=(), pending: str = ''):
        if not job_id:
            raise ValueError('Job ID cannot be None or empty')
        if execution is None:
            raise TypeError('Job execution cannot be None type')

        self.id = job_id
        self.execution = execution
        self.observers = list(observers)
        self.pending = pending

    def __repr__(self):
        return "{}({!r}, {!r}, {!r}, {!r})".format(
            self.__class__.__name__, self.id, self.execution, self.observers, self.pending)


class JobInstance(abc.ABC):

    @property
    @abc.abstractmethod
    def job_id(self) -> str:
        """Identifier of the job of this instance"""

    @property
    @abc.abstractmethod
    def instance_id(self) -> str:
        """Identifier of this instance"""

    @property
    @abc.abstractmethod
    def lifecycle(self):
        """Execution lifecycle of this instance"""

    @property
    @abc.abstractmethod
    def progress(self):
        """Current progress of the job or None if not supported"""

    @property
    @abc.abstractmethod
    def exec_error(self) -> ExecutionError:
        """Job execution error if occurred otherwise None"""

    def __repr__(self):
        return "{}({!r}, {!r}, {!r}, {!r})".format(
            self.__class__.__name__, self.instance_id, self.job_id, self.lifecycle, self.exec_error)


class JobInstanceData(JobInstance):

    def __init__(self, job_id: str, instance_id: str, lifecycle, progress, exec_error: ExecutionError):
        self._job_id = job_id
        self._instance_id = instance_id
        self._lifecycle = lifecycle
        self._progress = progress
        self._exec_error = exec_error

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def lifecycle(self):
        return self._lifecycle

    @property
    def progress(self):
        return self._progress

    @property
    def exec_error(self) -> ExecutionError:
        return self._exec_error


class JobControl(JobInstance):

    @abc.abstractmethod
    def release(self, pending: str) -> bool:
        """
        Trigger job execution waiting for a given condition. Ignore if the instance doesn't wait for the condition.
        :param pending: name of the condition
        :return: whether job has been released
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


class ExecutionStateObserver(abc.ABC):

    @abc.abstractmethod
    def state_update(self, job_instance):
        """This method is called when state is changed."""
