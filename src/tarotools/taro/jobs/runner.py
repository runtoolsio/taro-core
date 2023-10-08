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

Implementation notes:
Always acquire locks in this order to prevent deadlocks: 1. State lock 2. State global lock
State lock must be hold whole duration between state change and observers notification to ensure the observers are
always notified in the correct order. Global lock can be released after the state change to unblock others ASAP.
"""

import contextlib
import copy
import logging
from collections import deque, Counter
from threading import RLock
from typing import List, Union, Tuple, Optional

from tarotools.taro import util
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


class RunnerJobInstance(RunnableJobInstance, ExecutionOutputObserver):

    def __init__(self, job_id, execution, coord=NoSync(), state_locker=lock.default_state_locker(),
                 *, instance_id=None, pending_group=None, **user_params):
        self._id = JobInstanceID(job_id, instance_id or util.unique_timestamp_hex())
        self._execution = execution
        coord = coord or NoSync()
        if pending_group:
            self._coord = CompositeSync(Latch(ExecutionState.PENDING), coord)
        else:
            self._coord = coord
        parameters = (execution.parameters or ()) + (coord.parameters or ())
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

        self._change_state(ExecutionState.CREATED)

    def _log(self, event: str, msg: str = '', *params):
        return ("event=[{}] instance=[{}] " + msg).format(event, self._id, *params)

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

    def add_state_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY, notify_on_register=False):
        with self._state_lock:
            self._state_notification.add_observer(observer, priority)
            if notify_on_register:
                self._state_notification.notify_state_changed(observer, self.create_snapshot())

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
        self._coord.release()

    def stop(self):
        """
        Cancel not yet started execution or stop started execution.
        Due to synchronous design there is a small window when an execution can be stopped before it is started.
        All execution implementations must cope with such scenario.
        """
        self._stopped_or_interrupted = True

        self._coord.release()
        if self._executing:
            self._execution.stop()

    def interrupted(self):
        """
        Cancel not yet started execution or interrupt started execution.
        Due to synchronous design there is a small window when an execution can be interrupted before it is started.
        All execution implementations must cope with such scenario.
        """
        self._stopped_or_interrupted = True

        self._coord.release()
        if self._executing:
            self._execution.interrupted()

    def add_warning(self, warning):
        self._warnings.update([warning.name])
        log.warning(self._log('new_warning', 'warning=[{}]', warning))
        # Lock?
        self._warning_notification.notify_all(self.create_snapshot(),
                                              WarnEventCtx(warning, self._warnings[warning.name]))

    def run(self):
        if self._lifecycle.state != ExecutionState.CREATED:
            raise UnexpectedStateError("The run method can be called only once")

        try:
            coordinated = self._coordinate()
        except Exception as e:
            log.error(self._log('coord_error'), exc_info=e)
            self._change_state(ExecutionState.ERROR)
            return

        if coordinated:
            self._executing = True
        else:
            return

        # Forward output from execution to the job instance for the instance's output listeners
        self._execution.add_output_observer(self)

        try:
            new_state = self._execution.execute()
            self._change_state(new_state)
        except Exception as e:
            exec_error = e if isinstance(e, ExecutionError) else ExecutionError.from_unexpected_error(e)
            self._change_state(exec_error.exec_state, exec_error)
        except KeyboardInterrupt:
            log.warning(self._log('keyboard_interruption'))
            # Assuming child processes received SIGINT, TODO different state on other platforms?
            self._change_state(ExecutionState.INTERRUPTED)
            raise
        except SystemExit as e:
            # Consider UNKNOWN (or new state DETACHED?) if there is possibility the execution is not completed
            state = ExecutionState.COMPLETED if e.code == 0 else ExecutionState.FAILED
            self._change_state(state)
            raise
        finally:
            self._execution.remove_output_observer(self)

    def _coordinate(self) -> bool:
        while True:
            # Always keep this order to prevent deadlock: 1. Local state lock 2. Global state lock
            try:
                self._state_lock.acquire()
                with self._global_state_locker() as global_lock:
                    if self._stopped_or_interrupted:
                        signal = Signal.TERMINATE
                        state = ExecutionState.CANCELLED
                    else:
                        signal = self._coord.set_signal(self.create_snapshot())
                        if signal is Signal.NONE:
                            raise UnexpectedStateError(f"Signal.NONE is not allowed value for signaling")

                        if signal is Signal.WAIT and self._released:
                            signal = Signal.CONTINUE

                        if signal is Signal.CONTINUE:
                            state = ExecutionState.RUNNING
                        else:
                            state = self._coord.exec_state
                            if not (state.has_flag(Flag.WAITING) or state.in_phase(Phase.TERMINAL)):
                                raise UnexpectedStateError(f"Unsupported state returned from sync: {state}")

                    if signal is Signal.WAIT and self.lifecycle.state == state:
                        self._state_lock.release()  # Opposite release order + premature state lock release - be careful
                        self._coord.wait_and_unlock(global_lock)  # Waiting state already set, now we can wait
                        new_job_inst = None
                        released = True
                    else:
                        # Shouldn't notify listener whilst holding the global lock, so first we only set the state...
                        new_job_inst: Optional[JobInst] = self._change_state(state, notify=False, use_global_lock=False)
                        released = False

                if new_job_inst:
                    # ...and now, when global lock is released (state lock still in hold), we can notify.
                    # If signal is wait, the cycle will repeat and the instance will finally start waiting
                    # (unless the condition for waiting changed meanwhile)
                    self._state_notification.notify_all_state_changed(new_job_inst)
            finally:
                if not released:
                    self._state_lock.release()

            if signal is Signal.WAIT:
                continue
            if signal is Signal.TERMINATE:
                return False

            return True

    def _change_state(self, new_state, exec_error: ExecutionError = None, *, notify=True, use_global_lock=True) \
            -> Optional[JobInst]:
        if exec_error:
            self._exec_error = exec_error
            if exec_error.exec_state == ExecutionState.ERROR or exec_error.unexpected_error:
                log.exception(self._log('job_error', "reason=[{}]", exec_error), exc_info=True)
            else:
                log.warning(self._log('job_not_completed', "reason=[{}]", exec_error))

        global_state_lock = self._global_state_locker if use_global_lock else contextlib.nullcontext
        # It is not necessary to lock all this code, but it would be if this method is not confined to one thread
        # However locking is still needed for correct creation of job info when create_snapshot method is called
        with self._state_lock:  # Always keep this order to prevent deadlock: 1. Local state lock 2. Global state lock
            with global_state_lock():
                prev_state = self._lifecycle.state
                if not self._lifecycle.set_state(new_state):
                    return None

                level = logging.WARN if new_state.has_flag(Flag.NONSUCCESS) else logging.INFO
                log.log(level, self._log('job_state_changed', "prev_state=[{}] new_state=[{}]",
                                         prev_state.name, new_state.name))
                # Be sure both new_state and exec_error are already set
                new_job_inst = self.create_snapshot()

            # Do not hold global lock during notification
            if notify:
                self._state_notification.notify_all_state_changed(new_job_inst)

            return new_job_inst

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
