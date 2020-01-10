"""
Implementation of job management framework based on :mod:`job` module.
"""

import logging
from datetime import datetime
from threading import Event
from typing import List

from taro.execution import ExecutionError, ExecutionState
from taro.job import JobInstance, ExecutionStateObserver

log = logging.getLogger(__name__)


def _instance_id(job) -> str:
    return job.id + "_" + format(int(datetime.utcnow().timestamp() * 1000), 'x')


def run(job):
    RunnerJobInstance(job).run()


class RunnerJobInstance(JobInstance):

    def __init__(self, job):
        self._id = _instance_id(job)
        self._job = job
        self._state = ExecutionState.NONE
        self._exec_error = None
        if job.wait:
            self._event = Event()

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
        if self._job.wait:
            self._set_state(ExecutionState.WAITING)
            self._event.wait()

        self._set_state(ExecutionState.TRIGGERED)
        try:
            new_state = self._job.execution.execute()
            self._set_state(new_state)
        except Exception as e:
            exec_error = e if isinstance(e, ExecutionError) else ExecutionError.from_unexpected_error(e)
            self._set_error(exec_error)
            self._set_state(exec_error.exec_state)

    def release(self, wait: str):
        if wait and self._job.wait == wait:
            self._event.set()

    # Inline?
    def _log(self, event: str, msg: str):
        return "event=[{}] job_id=[{}] instance_id=[{}] {}".format(event, self._job.id, self._id, msg)

    def _set_state(self, exec_state):
        if not exec_state or exec_state == ExecutionState.NONE or self._state == exec_state:
            return

        prev_state, self._state = self._state, exec_state
        level = logging.WARN if self._state.is_failure() else logging.INFO
        log.log(level, self._log('job_state_changed', "new_state=[{}] prev_state=[{}]".format(
            self._state.name.lower(), prev_state.name.lower())))

        self._notify_observers()

    def _set_error(self, exec_error: ExecutionError):
        self._exec_error = exec_error
        if exec_error.exec_state == ExecutionState.ERROR or exec_error.unexpected_error:
            log.exception(self._log('job_error', "reason=[{}]".format(exec_error)), exc_info=True)
        else:
            log.warning(self._log('job_failed', "reason=[{}]".format(exec_error)))

    def _notify_observers(self):
        for observer in (self._job.observers + _observers):
            # noinspection PyBroadException
            try:
                observer.notify(self)
            except Exception:
                log.exception("event=[observer_exception]")


_observers: List[ExecutionStateObserver] = []


def register_observer(observer):
    _observers.append(observer)


def deregister_observer(observer):
    _observers.remove(observer)
