"""
Implementation of job management framework based on :mod:`job` module.
"""
import copy
import logging
import re
from collections import deque
from threading import Lock, Event, RLock
from typing import List, Union, Optional, Callable

from taro import util, persistence, client
from taro.err import IllegalStateError
from taro.execution import ExecutionError, ExecutionState, ExecutionLifecycleManagement, ExecutionOutputObserver
from taro.job import ExecutionStateObserver, JobControl, JobInfo, WarningEvent, WarningObserver, JobOutputObserver

log = logging.getLogger(__name__)


def run(job_id, execution, no_overlap: bool = False):
    instance = RunnerJobInstance(job_id, execution, no_overlap=no_overlap)
    instance.run()
    return instance


class RunnerJobInstance(JobControl, ExecutionOutputObserver):

    def __init__(self, job_id, execution, *, no_overlap: bool = False):
        self._job_id = job_id
        self._execution = execution
        self._no_overlap = no_overlap
        self._instance_id: str = util.unique_timestamp_hex()
        self._lifecycle: ExecutionLifecycleManagement = ExecutionLifecycleManagement()
        self._last_output = deque(maxlen=10)
        self._exec_error = None
        self._executing = False
        self._stopped_or_interrupted: bool = False
        self._executing_flag_lock: Lock = Lock()
        self._state_lock: RLock = RLock()
        self._latch: Optional[Event] = None
        self._latch_wait_state: Optional[ExecutionState] = None
        self._warnings = {}
        self._state_observers = []
        self._warning_observers = []
        self._output_observers = []

        self._state_change(ExecutionState.CREATED)

    def __repr__(self):
        return "{}({!r}, {!r})".format(
            self.__class__.__name__, self._job_id, self._execution)

    def create_latch(self, wait_state: ExecutionState):
        if not wait_state.is_before_execution():
            raise ValueError(str(wait_state) + "is not before execution state!")
        with self._executing_flag_lock:
            if self._executing:
                raise IllegalStateError("The latch cannot be created because the job has been already started")
            if self._stopped_or_interrupted:
                raise IllegalStateError("The latch cannot be created because the job execution has already ended")
            if self._latch:
                raise IllegalStateError("The latch has been already created")
            self._latch_wait_state = wait_state
            self._latch = Event()

        return self._latch.set

    @property
    def instance_id(self) -> str:
        return self._instance_id

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def lifecycle(self):
        return self._lifecycle

    @property
    def status(self):
        return self._execution.status()

    @property
    def last_output(self) -> List[str]:
        return list(self._last_output)

    @property
    def warnings(self):
        return self._warnings.values()

    @property
    def exec_error(self) -> Union[ExecutionError, None]:
        return self._exec_error

    def create_info(self):
        with self._state_lock:
            return JobInfo(
                self.job_id, self.instance_id, copy.deepcopy(self._lifecycle), self.status, self.warnings,
                self.exec_error)

    def add_state_observer(self, observer):
        self._state_observers.append(observer)

    def remove_state_observer(self, observer):
        self._state_observers.remove(observer)

    def add_warning_observer(self, observer):
        self._warning_observers.append(observer)

    def remove_warning_observer(self, observer):
        self._warning_observers.remove(observer)

    def stop(self):
        """
        Cancel not yet started execution or stop started execution.
        Due to synchronous design there is a small window when an execution can be stopped before it is started.
        All execution implementations must cope with such scenario.
        """
        with self._executing_flag_lock:
            self._stopped_or_interrupted = True

        if self._latch:
            self._latch.set()
        if self._executing:
            self._execution.stop()

    def interrupt(self):
        """
        Cancel not yet started execution or interrupt started execution.
        Due to synchronous design there is a small window when an execution can be interrupted before it is started.
        All execution implementations must cope with such scenario.
        """
        with self._executing_flag_lock:
            self._stopped_or_interrupted = True

        if self._latch:
            self._latch.set()
        if self._executing:
            self._execution.interrupt()

    def add_warning(self, warning):
        prev_warn = self._warnings.get(warning.id)
        if prev_warn == warning:
            return False  # No change

        self._warnings[warning.id] = warning
        if prev_warn:
            event = WarningEvent.WARNING_UPDATED
            log.warning('event=[updated_warning] new_warning=[%s] previous_warning=[%s]', warning, prev_warn)
        else:
            event = WarningEvent.NEW_WARNING
            log.warning('event=[new_warning] warning=[%s]', warning)

        self._notify_warning_observers(self.create_info(), warning, event)
        return True

    def run(self):
        for disabled in persistence.read_disabled_jobs():
            if (disabled.regex and re.compile(disabled.job_id).match(self.job_id)) or disabled.job_id == self.job_id:
                self._state_change(ExecutionState.DISABLED)
                return

        if self._latch and not self._stopped_or_interrupted:
            self._state_change(self._latch_wait_state)  # TODO Race condition?
            self._latch.wait()

        # Is variable assignment atomic? Better be sure with lock:
        # https://stackoverflow.com/questions/2291069/is-python-variable-assignment-atomic
        with self._executing_flag_lock:
            self._executing = not self._stopped_or_interrupted

        if not self._executing:
            self._state_change(ExecutionState.CANCELLED)
            return

        try:
            if self._no_overlap and any(j for j in client.read_jobs_info()
                                        if j.job_id == self.job_id and j.instance_id != self._instance_id):
                self._state_change(ExecutionState.SKIPPED)
                return
        except Exception as e:
            log.warning("event=[read_jobs_info_error] error=[%s]", e)

        self._state_change(ExecutionState.TRIGGERED if self._execution.is_async() else ExecutionState.RUNNING)
        try:
            new_state = self._execution.execute()
            self._state_change(new_state)
        except Exception as e:
            exec_error = e if isinstance(e, ExecutionError) else ExecutionError.from_unexpected_error(e)
            self._state_change(exec_error.exec_state, exec_error)

    # Inline?
    def _log(self, event: str, msg: str):
        return "event=[{}] job_id=[{}] instance_id=[{}] {}".format(event, self._job_id, self._instance_id, msg)

    def _state_change(self, new_state, exec_error: ExecutionError = None):
        # It is not necessary to lock all this code, but it would be if this method is not confined to one thread
        # However locking is still needed for correct creation of job info when job_info method is called (anywhere)
        job_info = None
        with self._state_lock:
            if exec_error:
                self._exec_error = exec_error
                if exec_error.exec_state == ExecutionState.ERROR or exec_error.unexpected_error:
                    log.exception(self._log('job_error', "reason=[{}]".format(exec_error)), exc_info=True)
                else:
                    log.warning(self._log('job_not_completed', "reason=[{}]".format(exec_error)))

            prev_state = self._lifecycle.state()
            if self._lifecycle.set_state(new_state):
                level = logging.WARN if new_state.is_failure() else logging.INFO
                log.log(level, self._log('job_state_changed', "new_state=[{}] prev_state=[{}]".format(
                    new_state.name, prev_state.name)))
                job_info = self.create_info()  # Be sure both new_state and exec_error are already set

        if job_info:
            if new_state.is_terminal():
                persistence.store_job(job_info)
            self._notify_state_observers(job_info)

    def _notify_state_observers(self, job_info: JobInfo):
        for observer in (self._state_observers + _state_observers):
            # noinspection PyBroadException
            try:
                if isinstance(observer, ExecutionStateObserver):
                    observer.state_update(job_info)
                elif callable(observer):
                    observer(job_info)
                else:
                    log.warning("event=[unsupported_state_observer] observer=[%s]", observer)
            except BaseException:
                log.exception("event=[state_observer_exception]")

    def _notify_warning_observers(self, job_info: JobInfo, warning, event: WarningEvent):
        for observer in (self._warning_observers + _warning_observers):
            # noinspection PyBroadException
            try:
                if isinstance(observer, WarningObserver):
                    observer.warning_update(job_info, warning, event)
                elif callable(observer):
                    observer(job_info, warning, event)
                else:
                    log.warning("event=[unsupported_warning_observer] observer=[%s]", observer)
            except BaseException:
                log.exception("event=[warning_observer_exception]")

    def output_update(self, output):
        """Executed when new output line is available"""
        self._last_output.append(output)
        self._notify_output_observers(self.create_info(), output)

    def _notify_output_observers(self, job_info: JobInfo, output):
        for observer in (self._output_observers + _output_observers):
            # noinspection PyBroadException
            try:
                if isinstance(observer, JobOutputObserver):
                    observer.output_update(job_info, output)
                elif callable(observer):
                    observer(job_info, output)
                else:
                    log.warning("event=[unsupported_output_observer] observer=[%s]", observer)
            except BaseException:
                log.exception("event=[output_observer_exception]")


_state_observers: List[Union[ExecutionStateObserver, Callable]] = []
_warning_observers: List[Union[WarningObserver, Callable]] = []
_output_observers: List[Union[JobOutputObserver, Callable]] = []


def register_state_observer(observer):
    _state_observers.append(observer)


def deregister_state_observer(observer):
    _state_observers.remove(observer)


def register_warning_observer(observer):
    _warning_observers.append(observer)


def deregister_warning_observer(observer):
    _warning_observers.remove(observer)


def register_output_observer(observer):
    _output_observers.append(observer)


def deregister_output_observer(observer):
    _output_observers.remove(observer)
