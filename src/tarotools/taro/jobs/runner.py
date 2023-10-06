"""
This module contains the default implementation of the `RunnableJobInstance` interface from the `inst` module.
A job instance is executed by calling the `run` method. The method call returns after the instance terminates.
It is possible to register observers directly in the module. These are then notified about events from all
job instances. An alternative for multi-observing is to use the featured context from the `featurize` module.

This implementation adds a few features not explicitly defined in the interface:

Coordination
------------
Job instances can be coordinated with each other or with any external logic. Coordinated instances are typically
affected in ways that might require them to wait for a condition, or they might be discarded if necessary. The
coordination logic heavily relies on global synchronization.

Some examples of coordination include:
  - Pending latch:
    Several instances can be started simultaneously by sending a release request to the corresponding group.
  - Serial execution:
    Instances of the same job or group execute sequentially.
  - Parallel execution limit:
    Only N number of instances can execute in parallel.
  - Overlap forbidden:
    Parallel execution of instances of the same job can be restricted.
  - Dependency:
    An instance can start only if another specific instance is active.

Global Synchronization
----------------------
For coordination to function correctly, a mechanism that allows the coordinator to utilize some form of
shared lock is essential. Job instances attempt to acquire this lock each time the coordination logic runs and
whenever there's a change in their state. Instances are permitted to change their state only after acquiring the lock.

"""

import contextlib
import copy
import logging
from collections import deque, Counter
from threading import RLock
from typing import List, Union, Tuple, Optional

from tarotools.taro import util, JobInstance
from tarotools.taro.jobs import lock
from tarotools.taro.jobs.execution import ExecutionError, ExecutionState, ExecutionLifecycleManagement, \
    ExecutionOutputObserver, \
    Phase, Flag, UnexpectedStateError
from tarotools.taro.jobs.inst import JobInst, WarnEventCtx, JobInstanceID, JobInstanceMetadata, \
    InstanceStateNotification, \
    InstanceOutputNotification, InstanceWarningNotification, RunnableJobInstance
from tarotools.taro.jobs.sync import NoSync, CompositeSync, Latch, Signal
from tarotools.taro.util.observer import DEFAULT_OBSERVER_PRIORITY

log = logging.getLogger(__name__)

_state_observers = InstanceStateNotification(logger=log)
_output_observers = InstanceOutputNotification(logger=log)
_warning_observers = InstanceWarningNotification(logger=log)


# TODO Move to the root package
def job_instance(job_id, execution, sync=NoSync(), state_locker=lock.default_state_locker(), *, instance_id=None,
                 pending_group=None, **user_params) \
        -> RunnableJobInstance:
    return _RunnerJobInstance(job_id, execution, sync, state_locker, instance_id=instance_id,
                              pending_group=pending_group, user_params=user_params)


# TODO Move to the root package
def run(job_id, execution, sync=NoSync(), state_locker=lock.default_state_locker(), *, instance_id=None,
        pending_group=None, **user_params) -> JobInstance:
    instance = job_instance(job_id, execution, sync, state_locker, instance_id=instance_id, pending_group=pending_group,
                            user_params=user_params)
    instance.run()
    return instance


# TODO Move to the root package
def job_instance_uncoordinated(job_id, execution, *, instance_id=None, pending_group=None, **user_params) \
        -> RunnableJobInstance:
    return _RunnerJobInstance(job_id, execution, state_locker=lock.NullStateLocker(), instance_id=instance_id,
                              pending_group=pending_group, user_params=user_params)


# TODO Move to the root package
def run_uncoordinated(job_id, execution, *, instance_id=None, pending_group=None, **user_params) -> JobInstance:
    instance = job_instance_uncoordinated(job_id, execution, instance_id=instance_id, pending_group=pending_group,
                                          user_params=user_params)
    instance.run()
    return instance


class _RunnerJobInstance(RunnableJobInstance, ExecutionOutputObserver):

    def __init__(self, job_id, execution, sync=NoSync(), state_locker=lock.default_state_locker(),
                 *, instance_id=None, pending_group=None, **user_params):
        self._id = JobInstanceID(job_id, instance_id or util.unique_timestamp_hex())
        self._execution = execution
        sync = sync or NoSync()
        if pending_group:
            self._sync = CompositeSync(Latch(ExecutionState.PENDING), sync)
        else:
            self._sync = sync
        parameters = (execution.parameters or ()) + (sync.parameters or ())
        self._metadata = JobInstanceMetadata(self._id, parameters, user_params, pending_group)
        self._global_state_locker = state_locker
        self._lifecycle: ExecutionLifecycleManagement = ExecutionLifecycleManagement()
        self._released = False
        self._last_output = deque(maxlen=10)  # TODO Max len configurable
        self._error_output = deque(maxlen=1000)  # TODO Max len configurable
        self._exec_error = None
        self._executing = False
        self._stopped_or_interrupted: bool = False
        self._state_lock: RLock = RLock()
        self._warnings = Counter()
        self._state_notification = InstanceStateNotification(log, _state_observers)
        self._warning_notification = InstanceWarningNotification(log, _warning_observers)
        self._output_notification = InstanceOutputNotification(log, _output_observers)

        self._change_state_and_notify(ExecutionState.CREATED)  # TODO Move somewhere post-init?

    @property
    def id(self):
        return self._id

    @property
    def metadata(self):
        return self._metadata

    @property
    def lifecycle(self):
        return self._lifecycle

    @property
    def tracking(self):
        return self._execution.tracking

    @property
    def status(self):
        return self._execution.status

    @property
    def last_output(self) -> List[Tuple[str, bool]]:
        return list(self._last_output)

    @property
    def error_output(self) -> List[str]:
        return list(self._error_output)

    @property
    def warnings(self):
        return dict(self._warnings)

    @property
    def exec_error(self) -> Union[ExecutionError, None]:
        return self._exec_error

    def create_snapshot(self):
        with self._state_lock:
            return JobInst(
                self.metadata,
                copy.deepcopy(self._lifecycle),
                self.tracking.copy() if self.tracking else None,
                self.status,
                self.error_output,
                self.warnings,
                self.exec_error)

    def add_state_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self._state_notification.add_observer(observer, priority)

    def remove_state_observer(self, observer):
        self._state_notification.remove_observer(observer)

    def add_warning_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self._warning_notification.add_observer(observer, priority)

    def remove_warning_observer(self, observer):
        self._warning_notification.remove_observer(observer)

    def add_output_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self._output_notification.add_observer(observer, priority)

    def remove_output_observer(self, observer):
        self._output_notification.remove_observer(observer)

    def release(self):
        self._released = True
        self._sync.release()

    def stop(self):
        """
        Cancel not yet started execution or stop started execution.
        Due to synchronous design there is a small window when an execution can be stopped before it is started.
        All execution implementations must cope with such scenario.
        """
        self._stopped_or_interrupted = True

        self._sync.release()
        if self._executing:
            self._execution.stop()

    def interrupted(self):
        """
        Cancel not yet started execution or interrupt started execution.
        Due to synchronous design there is a small window when an execution can be interrupted before it is started.
        All execution implementations must cope with such scenario.
        """
        self._stopped_or_interrupted = True

        self._sync.release()
        if self._executing:
            self._execution.interrupted()

    def add_warning(self, warning):
        self._warnings.update([warning.name])
        log.warning('event=[new_warning] warning=[%s]', warning)
        # Lock?
        self._warning_notification.notify_all(self.create_snapshot(),
                                              WarnEventCtx(warning, self._warnings[warning.name]))

    def run(self):
        # TODO Check executed only once

        try:
            synchronized = self._synchronize()
        except Exception as e:
            log.error('event=[sync_error]', exc_info=e)
            self._change_state_and_notify(ExecutionState.ERROR)
            return

        if synchronized:
            self._executing = True
        else:
            return

        # Forward output from execution to the job instance for the instance's output listeners
        self._execution.add_output_observer(self)

        try:
            new_state = self._execution.execute()
            self._change_state_and_notify(new_state)
        except Exception as e:
            exec_error = e if isinstance(e, ExecutionError) else ExecutionError.from_unexpected_error(e)
            self._change_state_and_notify(exec_error.exec_state, exec_error)
        except KeyboardInterrupt:
            log.warning("event=[keyboard_interruption]")
            # Assuming child processes received SIGINT, TODO different state on other platforms?
            self._change_state_and_notify(ExecutionState.INTERRUPTED)
            raise
        except SystemExit as e:
            # Consider UNKNOWN (or new state DETACHED?) if there is possibility the execution is not completed
            state = ExecutionState.COMPLETED if e.code == 0 else ExecutionState.FAILED
            self._change_state_and_notify(state)
            raise
        finally:
            self._execution.remove_output_observer(self)

    def _synchronize(self) -> bool:
        while True:
            with self._global_state_locker() as state_lock:
                if self._stopped_or_interrupted:
                    signal = Signal.TERMINATE
                    state = ExecutionState.CANCELLED
                else:
                    signal = self._sync.set_signal(self.create_snapshot())
                    if signal is Signal.NONE:
                        assert False  # TODO raise exception

                    if signal is Signal.WAIT and self._released:
                        signal = Signal.CONTINUE

                    if signal is Signal.CONTINUE:
                        state = ExecutionState.RUNNING
                    else:
                        state = self._sync.exec_state
                        if not (state.has_flag(Flag.WAITING) or state.in_phase(Phase.TERMINAL)):
                            raise UnexpectedStateError(f"Unsupported state returned from sync: {state}")

                # If wait and not the same state then the state must be changed first,
                # then -> release the lock + notify observers and repeat
                if signal is Signal.WAIT and self.lifecycle.state == state:
                    self._sync.wait_and_unlock(state_lock)  # Waiting state already set, now we can wait
                    new_job_inst = None
                else:
                    new_job_inst: Optional[JobInst] = self._change_state(state, use_global_lock=False)

            # Lock released -> do not hold lock when executing observers
            if new_job_inst:
                self._state_notification.notify_state_changed(new_job_inst)
            if signal is Signal.WAIT:
                continue
            if signal is Signal.TERMINATE:
                return False

            return True

    # Inline?
    def _log(self, event: str, msg: str):
        return "event=[{}] instance=[{}] {}".format(event, self._id, msg)

    def _change_state(self, new_state, exec_error: ExecutionError = None, *, use_global_lock=True) -> Optional[JobInst]:
        if exec_error:
            self._exec_error = exec_error
            if exec_error.exec_state == ExecutionState.ERROR or exec_error.unexpected_error:
                log.exception(self._log('job_error', "reason=[{}]".format(exec_error)), exc_info=True)
            else:
                log.warning(self._log('job_not_completed', "reason=[{}]".format(exec_error)))

        global_state_lock = self._global_state_locker() if use_global_lock else contextlib.nullcontext()
        # It is not necessary to lock all this code, but it would be if this method is not confined to one thread
        # However locking is still needed for correct creation of job info when job_info method is called (anywhere)
        with global_state_lock:  # Always keep order 1. Global state lock 2. Local state lock
            with self._state_lock:
                prev_state = self._lifecycle.state
                if not self._lifecycle.set_state(new_state):
                    return None

                level = logging.WARN if new_state.has_flag(Flag.NONSUCCESS) else logging.INFO
                log.log(level, self._log('job_state_changed', "prev_state=[{}] new_state=[{}]".format(
                    prev_state.name, new_state.name)))
                # Be sure both new_state and exec_error are already set
                return self.create_snapshot()

    def _change_state_and_notify(self, new_state, exec_error: ExecutionError = None, *, use_global_lock=True):
        new_job_inst = self._change_state(new_state, exec_error, use_global_lock=use_global_lock)
        if new_job_inst:
            self._state_notification.notify_state_changed(new_job_inst)

    def execution_output_update(self, output, is_error):
        """Executed when new output line is available"""
        self._last_output.append((output, is_error))
        if is_error:
            self._error_output.append(output)
        self._output_notification.notify_all(self.create_snapshot(), output, is_error)


def register_state_observer(observer, priority=DEFAULT_OBSERVER_PRIORITY):
    global _state_observers
    _state_observers.add_observer(observer, priority)


def deregister_state_observer(observer):
    global _state_observers
    _state_observers.remove_observer(observer)


def register_warning_observer(observer, priority=DEFAULT_OBSERVER_PRIORITY):
    global _warning_observers
    _warning_observers.add_observer(observer, priority)


def deregister_warning_observer(observer):
    global _warning_observers
    _warning_observers.remove_observer(observer)


def register_output_observer(observer, priority=DEFAULT_OBSERVER_PRIORITY):
    global _output_observers
    _output_observers.add_observer(observer, priority)


def deregister_output_observer(observer):
    global _output_observers
    _output_observers.remove_observer(observer)
