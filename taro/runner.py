import abc
import logging
from datetime import datetime
from typing import List

from taro.execution import ExecutionError, ExecutionState
from taro.job import Job

log = logging.getLogger(__name__)


def run(job):
    instance = JobInstance(job)
    run_instance(instance)
    return instance


def run_instance(instance):
    instance.run()


def _instance_id(job) -> str:
    return job.id + "_" + format(int(datetime.utcnow().timestamp() * 1000), 'x')


class JobInstance:

    def __init__(self, job):
        self._id = _instance_id(job)
        self._job = job
        self._state = ExecutionState.NONE
        self._exec_error = None

    @property
    def id(self):
        return self._id

    @property
    def job_id(self):
        return self._job.id

    @property
    def state(self):
        return self._state

    @property
    def exec_error(self):
        return self._exec_error

    def run(self):
        self._set_state(ExecutionState.TRIGGERED)
        try:
            new_state = self._job.execution.execute()
            self._set_state(new_state)
        except Exception as e:
            exec_error = e if isinstance(e, ExecutionError) else ExecutionError.from_unexpected_error(e)
            self._set_error(exec_error)
            self._set_state(exec_error.exec_state)

    # Inline?
    def _log(self, event: str, msg: str):
        return "event=[{}] job_id=[{}] exec_id=[{}] {}".format(event, self._job.id, self._id, msg)

    def _set_state(self, exec_state):
        if not exec_state or exec_state == ExecutionState.NONE or self._state == exec_state:
            return

        prev_state, self._state = self._state, exec_state
        level = logging.WARN if self._state.is_failure() else logging.INFO
        log.log(level, self._log('job_state_changed',
                                 "new_state=[{}] prev_state=[{}]".format(
                                     self._state.name.lower(), prev_state.name.lower())))

        self._notify_observers()

    def _set_error(self, exec_error: ExecutionError):
        self._exec_error = exec_error
        if exec_error.exec_state == ExecutionState.ERROR or exec_error.unexpected_error:
            log.exception(self._log('job_error', "reason=[{}]".format(exec_error)), exc_info=True)
        else:
            log.warning(self._log('job_failed', "reason=[{}]".format(exec_error)))

    def _notify_observers(self):
        for observer in _observers:
            # noinspection PyBroadException
            try:
                observer.notify(self._job, self._state, self._exec_error)
            except Exception:
                log.exception("event=[observer_exception]")


class ExecutionStateObserver(abc.ABC):

    @abc.abstractmethod
    def notify(self, job: Job, exec_state: ExecutionState, exec_error=None):
        """This method is called when state is changed."""


_observers: List[ExecutionStateObserver] = []


def register_observer(observer):
    _observers.append(observer)


def deregister_observer(observer):
    _observers.remove(observer)


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
