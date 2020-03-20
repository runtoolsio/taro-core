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

from taro.execution import ExecutionState, ExecutionError


class Job:
    def __init__(self, job_id: str, execution, observers=(), wait: str = ''):
        if not job_id:
            raise ValueError('Job ID cannot be None or empty')
        if execution is None:
            raise TypeError('Job execution cannot be None type')

        self.id = job_id
        self.execution = execution
        self.observers = list(observers)
        self.wait = wait

    def __repr__(self):
        return "{}({!r}, {!r}, {!r})".format(
            self.__class__.__name__, self.id, self.execution, self.observers)


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
    def release(self, wait: str) -> bool:
        """
        Trigger job execution waiting for a given condition. Ignore if the instance doesn't wait for the condition.
        :param wait: name of the condition
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
    def notify(self, job_instance):
        """This method is called when state is changed."""


class ExecutionStateListener(ExecutionStateObserver):

    def __init__(self):
        self.state_to_method = {
            ExecutionState.TRIGGERED: self.on_triggered,
            ExecutionState.STARTED: self.on_started,
            ExecutionState.COMPLETED: self.on_completed,
            ExecutionState.START_FAILED: self.start_failed,
            ExecutionState.FAILED: self.on_failed,
        }  # TODO all states

    # noinspection PyMethodMayBeStatic
    def is_observing(self, _):
        """
        Whether this listener listens to the changes of the given job instance
        :param _: job instance
        """
        return True

    def notify(self, job_instance):
        """
        This method is called when state is changed.

        It is responsible to delegate to corresponding on_* listening method.
        """

        if not self.is_observing(job_instance):
            return

        self.state_to_method[job_instance.state](job_instance)

    @abc.abstractmethod
    def on_triggered(self, job_instance):
        """Job initialized but execution no yet started"""

    @abc.abstractmethod
    def on_started(self, job_instance):
        """Job execution started"""

    @abc.abstractmethod
    def on_completed(self, job_instance):
        """Job execution successfully completed"""

    @abc.abstractmethod
    def start_failed(self, job_instance):
        """Starting of the job failed -> job did not run"""

    @abc.abstractmethod
    def on_failed(self, job_instance):
        """Job had started but the execution failed"""
