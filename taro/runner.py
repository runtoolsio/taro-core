import abc
import logging
from typing import List

from taro.execution import ExecutionError, ExecutionState
from taro.job import Job

log = logging.getLogger(__name__)


def run(job: Job):
    JobInstance(job).run()


class JobInstance:

    def __init__(self, job: Job):
        self.job = job
        self.id = 'uuid'  # TODO real
        self.state = ExecutionState.NONE
        self.exec_error = None

    # Inline?
    def _log(self, event: str, msg: str):
        return "event=[{}] job_id=[{}] exec_id=[{}] {}".format(event, self.job.id, self.id, msg)

    def _set_state(self, exec_state, exec_error=None):
        if not exec_state or exec_state == ExecutionState.NONE or self.state == exec_state:
            return

        if exec_error:
            self.exec_error = exec_error
            log.exception(self._log('job_failed', "reason=[{}]".format(exec_error)), exc_info=True)

        prev_state, self.state = self.state, exec_state
        level = logging.WARN if self.state.is_failure() else logging.INFO
        log.log(level, self._log('job_state_changed',
                                 "new_state=[{}] prev_state=[{}]".format(
                                     self.state.name.lower(), prev_state.name.lower())))

        self._notify_observers()

    def _notify_observers(self):
        for observer in _observers:
            # noinspection PyBroadException
            try:
                observer.notify(self.job, self.state, self.exec_error)
            except Exception:
                log.exception("event=[observer_exception]")

    def run(self):
        self._set_state(ExecutionState.TRIGGERED)
        try:
            new_state = self.job.execution.execute_catch_exc()
            self._set_state(new_state)
        except ExecutionError as e:
            new_state = ExecutionState.NOT_STARTED if e.not_started else ExecutionState.FAILED
            self._set_state(new_state, e)


class ExecutionStateObserver(abc.ABC):

    @abc.abstractmethod
    def notify(self, job: Job, exec_state: ExecutionState, exec_error=None):
        """This method is called when state is changed."""


_observers: List[ExecutionStateObserver] = []


def register_observer(observer):
    _observers.append(observer)


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
    def is_observing(self, _: Job):
        """Whether this listener listens to the changes of the given job"""
        return True

    def notify(self, job: Job, exec_state: ExecutionState, exec_error=None):
        """
        This method is called when state is changed.

        It is responsible to delegate to corresponding on_* listening method.
        """

        if not self.is_observing(job):
            return

        notify_method = self.state_to_method[exec_state]
        if exec_state.is_failure():
            notify_method(job, exec_error)
        else:
            notify_method(job)

    @abc.abstractmethod
    def on_triggered(self, job: Job):
        """TODO"""

    @abc.abstractmethod
    def on_started(self, job: Job):
        """Send notification about successful start of an asynchronous job"""

    @abc.abstractmethod
    def on_completed(self, job: Job):
        """Send notification about successful completion of a job"""

    @abc.abstractmethod
    def on_not_started(self, job: Job, exec_error: ExecutionError):
        """Send notification about failed start of an asynchronous job"""

    @abc.abstractmethod
    def on_failed(self, job: Job, exec_error: ExecutionError):
        """Send notification about failed execution of a job"""
