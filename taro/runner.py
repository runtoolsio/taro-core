"""
Implementation of job management framework based on :mod:`job` module.
"""
import datetime
import logging
from threading import Lock, Event
from typing import List, Tuple, Union

from taro import util
from taro.execution import ExecutionError, ExecutionState
from taro.job import ExecutionStateObserver, JobControl

log = logging.getLogger(__name__)


def _instance_id(job) -> str:
    return job.id + "_" + util.unique_timestamp_hex()


def run(job):
    instance = RunnerJobInstance(job)
    instance.run()
    return instance


class RunnerJobInstance(JobControl):

    def __init__(self, job):
        self._instance_id: str = _instance_id(job)
        self._job = job
        self._state_changes: List[Tuple[ExecutionState, datetime.datetime]] = []
        self._exec_error = None
        self._executing = False
        self._stopped_or_interrupted: bool = False
        self._executing_flag_lock: Lock = Lock()
        if job.wait:
            self._wait_condition: Event = Event()

        self._set_state(ExecutionState.CREATED)

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def job_id(self) -> str:
        return self._job.id

    @property
    def state(self) -> ExecutionState:
        if self._state_changes:
            return self._state_changes[-1][0]
        else:
            return ExecutionState.NONE

    @property
    def state_changes(self) -> List[Tuple[ExecutionState, datetime.datetime]]:
        return self._state_changes

    def states(self) -> List[ExecutionState]:
        return [state for state, _ in self.state_changes]

    @property
    def exec_error(self) -> Union[ExecutionError, None]:
        return self._exec_error

    def run(self):
        if self._job.wait and not self._stopped_or_interrupted:
            self._set_state(ExecutionState.WAITING)
            self._wait_condition.wait()

        # Is variable assignment atomic? Better be sure with lock:
        # https://stackoverflow.com/questions/2291069/is-python-variable-assignment-atomic
        with self._executing_flag_lock:
            self._executing = not self._stopped_or_interrupted

        if not self._executing:
            self._set_state(ExecutionState.CANCELLED)
            return

        self._set_state(ExecutionState.TRIGGERED)
        try:
            new_state = self._job.execution.execute()
            self._set_state(new_state)
        except Exception as e:
            exec_error = e if isinstance(e, ExecutionError) else ExecutionError.from_unexpected_error(e)
            self._set_error(exec_error)
            self._set_state(exec_error.exec_state)

    def release(self, wait: str) -> bool:
        if wait and self._job.wait == wait and not self._wait_condition.is_set():
            self._wait_condition.set()
            return True
        else:
            return False

    def stop(self):
        """
        Cancel not yet started execution or stop started execution.
        Due to synchronous design there is a small window when an execution can be stopped before it is started.
        All execution implementations must cope with such scenario.
        """
        with self._executing_flag_lock:
            self._stopped_or_interrupted = True

        if self._job.wait:
            self._wait_condition.set()
        if self._executing:
            self._job.execution.stop()

    def interrupt(self):
        """
        Cancel not yet started execution or interrupt started execution.
        Due to synchronous design there is a small window when an execution can be interrupted before it is started.
        All execution implementations must cope with such scenario.
        """
        with self._executing_flag_lock:
            self._stopped_or_interrupted = True

        if self._job.wait:
            self._wait_condition.set()
        if self._executing:
            self._job.execution.interrupt()

    # Inline?
    def _log(self, event: str, msg: str):
        return "event=[{}] job_id=[{}] instance_id=[{}] {}".format(event, self._job.id, self._instance_id, msg)

    def _set_state(self, new_state):
        if not new_state or new_state == ExecutionState.NONE or self.state == new_state:
            return

        prev_state = self.state
        self._state_changes.append((new_state, datetime.datetime.utcnow()))
        level = logging.WARN if new_state.is_failure() else logging.INFO
        log.log(level, self._log('job_state_changed', "new_state=[{}] prev_state=[{}]".format(
            new_state.name.lower(), prev_state.name.lower())))

        self._notify_observers()

    def _set_error(self, exec_error: ExecutionError):
        self._exec_error = exec_error
        if exec_error.exec_state == ExecutionState.ERROR or exec_error.unexpected_error:
            log.exception(self._log('job_error', "reason=[{}]".format(exec_error)), exc_info=True)
        else:
            log.warning(self._log('job_not_completed', "reason=[{}]".format(exec_error)))

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
