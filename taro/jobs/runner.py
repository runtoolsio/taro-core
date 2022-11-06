"""
Implementation of job management framework based on :mod:`job` module.
"""
import copy
import logging
from collections import deque, Counter
from threading import Event, RLock
from typing import List, Union, Optional, Callable

import taro.client
from taro import util
from taro.err import IllegalStateError
from taro.jobs import persistence
from taro.jobs.execution import ExecutionError, ExecutionState, ExecutionLifecycleManagement, ExecutionOutputObserver
from taro.jobs.job import ExecutionStateObserver, JobInstance, JobInfo, WarningObserver, JobOutputObserver, Warn, \
    WarnEventCtx, JobInstanceID

log = logging.getLogger(__name__)


def run(job_id, execution, no_overlap: bool = False):
    instance = RunnerJobInstance(job_id, execution, no_overlap=no_overlap)
    instance.run()
    return instance


class RunnerJobInstance(JobInstance, ExecutionOutputObserver):

    def __init__(self, job_id, execution, *, no_overlap: bool = False, depends_on=None, **params):
        self._id = JobInstanceID(job_id, util.unique_timestamp_hex())
        self._params = params
        self._execution = execution
        self._no_overlap = no_overlap
        self._depends_on = depends_on
        self._lifecycle: ExecutionLifecycleManagement = ExecutionLifecycleManagement()
        self._last_output = deque(maxlen=10)
        self._exec_error = None
        self._executing = False
        self._stopped_or_interrupted: bool = False
        self._state_lock: RLock = RLock()
        self._latch: Optional[Event] = None
        self._latch_wait_state: Optional[ExecutionState] = None
        self._warnings = Counter()
        self._state_observers = []
        self._warning_observers = []
        self._output_observers = []

        self._state_change(ExecutionState.CREATED)

    def create_latch(self, wait_state: ExecutionState):
        if not wait_state.is_before_execution():
            raise ValueError(str(wait_state) + "is not before execution state!")
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
    def id(self):
        return self._id

    @property
    def lifecycle(self):
        return self._lifecycle

    @property
    def status(self):
        return self._execution.status

    @property
    def last_output(self) -> List[str]:
        return list(self._last_output)

    @property
    def warnings(self):
        return dict(self._warnings)

    @property
    def exec_error(self) -> Union[ExecutionError, None]:
        return self._exec_error

    def create_info(self):
        with self._state_lock:
            return JobInfo(self._id, copy.deepcopy(self._lifecycle), self.status, self.warnings, self.exec_error,
                           **self._params)

    def add_state_observer(self, observer):
        self._state_observers.append(observer)

    def remove_state_observer(self, observer):
        self._state_observers.remove(observer)

    def add_warning_observer(self, observer):
        self._warning_observers.append(observer)

    def remove_warning_observer(self, observer):
        self._warning_observers.remove(observer)

    def add_output_observer(self, observer):
        self._output_observers.append(observer)

    def remove_output_observer(self, observer):
        self._output_observers.remove(observer)

    def stop(self):
        """
        Cancel not yet started execution or stop started execution.
        Due to synchronous design there is a small window when an execution can be stopped before it is started.
        All execution implementations must cope with such scenario.
        """
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
        self._stopped_or_interrupted = True

        if self._latch:
            self._latch.set()
        if self._executing:
            self._execution.interrupt()

    def add_warning(self, warning):
        self._warnings.update([warning.name])
        log.warning('event=[new_warning] warning=[%s]', warning)
        self._notify_warning_observers(self.create_info(), warning, WarnEventCtx(self._warnings[warning.name]))  # Lock?

    def run(self):
        # TODO Check executed only once

        # Forward output from execution to the job instance for the instance's output listeners
        self._execution.add_output_observer(self)

        if self._latch and not self._stopped_or_interrupted:
            self._state_change(self._latch_wait_state)  # TODO Race condition?
            self._latch.wait()

        self._executing = not self._stopped_or_interrupted

        if not self._executing:
            self._state_change(ExecutionState.CANCELLED)
            return

        if self._no_overlap or self._depends_on:
            try:
                jobs = taro.client.read_jobs_info()

                if self._no_overlap and any(j for j in jobs
                                            if j.job_id == self.job_id and j.instance_id != self.instance_id):
                    self._state_change(ExecutionState.SKIPPED)
                    return

                if self._depends_on and not any(j for j in jobs
                                                if any(j.matches(dependency) for dependency in self._depends_on)):
                    self._state_change(ExecutionState.DEPENDENCY_NOT_RUNNING)
                    return
            except Exception as e:
                log.warning("event=[overlap_check_failed] error=[%s]", e)

        self._state_change(ExecutionState.TRIGGERED if self._execution.is_async else ExecutionState.RUNNING)
        try:
            new_state = self._execution.execute()
            self._state_change(new_state)
        except Exception as e:
            exec_error = e if isinstance(e, ExecutionError) else ExecutionError.from_unexpected_error(e)
            self._state_change(exec_error.exec_state, exec_error)
        except SystemExit as e:
            state = ExecutionState.COMPLETED if e.code == 0 else ExecutionState.FAILED  # TODO Different states?
            self._state_change(state)
            raise

    # Inline?
    def _log(self, event: str, msg: str):
        return "event=[{}] instance=[{}] {}".format(event, self._id, msg)

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
                level = logging.WARN if new_state.is_failure() or new_state.is_unexecuted() else logging.INFO
                log.log(level, self._log('job_state_changed', "prev_state=[{}] new_state=[{}]".format(
                    prev_state.name, new_state.name)))
                job_info = self.create_info()  # Be sure both new_state and exec_error are already set

        if job_info:
            if new_state.is_terminal() and persistence.is_enabled():
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

    def _notify_warning_observers(self, job_info: JobInfo, warning: Warn, event_ctx: WarnEventCtx):
        for observer in (self._warning_observers + _warning_observers):
            # noinspection PyBroadException
            try:
                if isinstance(observer, WarningObserver):
                    observer.new_warning(job_info, warning, event_ctx)
                elif callable(observer):
                    observer(job_info, warning, event_ctx)
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
