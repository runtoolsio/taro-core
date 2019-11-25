import abc

from taro.execution import ExecutionState, ExecutionError


class Job:
    def __init__(self, job_id: str, execution, observers=()):
        if not job_id:
            raise ValueError('Job ID cannot be None or empty')
        if execution is None:
            raise TypeError('Job execution cannot be None type')

        self.id = job_id
        self.execution = execution
        self.observers = list(observers)

    def __repr__(self):
        return "{}({!r}, {!r}, {!r})".format(
            self.__class__.__name__, self.id, self.execution, self.observers)


class JobInstance(abc.ABC):

    @property
    @abc.abstractmethod
    def id(self) -> str:
        """Identifier of this instance"""

    @property
    @abc.abstractmethod
    def job_id(self) -> str:
        """Identifier of the job of this instance"""

    @property
    @abc.abstractmethod
    def state(self) -> ExecutionState:
        """Current job execution state"""

    @property
    @abc.abstractmethod
    def exec_error(self) -> ExecutionError:
        """Job execution error if occurred otherwise None"""

    def __repr__(self):
        return "{}({!r}, {!r}, {!r}, {!r})".format(
            self.__class__.__name__, self.id, self.job_id, self.state, self.exec_error)


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
            ExecutionState.NOT_STARTED: self.on_not_started,
            ExecutionState.FAILED: self.on_failed,
        }

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
    def on_not_started(self, job_instance):
        """Starting of the job failed -> job did not run"""

    @abc.abstractmethod
    def on_failed(self, job_instance):
        """Job had started but the execution failed"""
