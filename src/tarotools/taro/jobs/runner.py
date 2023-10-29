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
shared lock is often essential. Job instances attempt to acquire this lock each time the coordination logic runs.
The lock remains held when the execution state changes. This is crucial because the current state of coordinated
instances often dictates the coordination action, and using a lock ensures that the 'check-then-act' sequence
on the execution states is atomic, preventing race conditions.


IMPLEMENTATION NOTES

State lock
----------
1. Atomic update of the job instance state (i.e. error object + failure state)
2. Consistent execution state notification order
3. No race condition on state observer notify on register

"""

import copy
import logging
from collections import deque, Counter
from typing import List, Union, Tuple

from tarotools.taro import util
from tarotools.taro.jobs.execution import ExecutionOutputObserver
from tarotools.taro.jobs.instance import JobInst, WarnEventCtx, JobInstanceID, JobInstanceMetadata, \
    InstancePhaseNotification, \
    InstanceOutputNotification, InstanceWarningNotification, RunnableJobInstance
from tarotools.taro.jobs.lifecycle import Phaser, MutableLifecycle, TerminationStatus, Flag, ExecutionError
from tarotools.taro.util.observer import DEFAULT_OBSERVER_PRIORITY

log = logging.getLogger(__name__)

_state_observers = InstancePhaseNotification(logger=log)
_output_observers = InstanceOutputNotification(logger=log)
_warning_observers = InstanceWarningNotification(logger=log)


class RunnerJobInstance(RunnableJobInstance, ExecutionOutputObserver):

    def __init__(self, job_id, phase_steps, *, instance_id=None, **user_params):
        self._id = JobInstanceID(job_id, instance_id or util.unique_timestamp_hex())
        self._phases = phase_steps
        self._phaser = Phaser(MutableLifecycle(), phase_steps)
        parameters = ()  # TODO
        self._metadata = JobInstanceMetadata(self._id, parameters, user_params)
        self._last_output = deque(maxlen=10)  # TODO Max len configurable
        self._error_output = deque(maxlen=1000)  # TODO Max len configurable
        self._executing = False
        self._exec_error = None
        self._warnings = Counter()
        self._state_notification = InstancePhaseNotification(log, _state_observers)
        self._warning_notification = InstanceWarningNotification(log, _warning_observers)
        self._output_notification = InstanceOutputNotification(log, _output_observers)

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
        return self._phaser.lifecycle

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
                copy.deepcopy(self.lifecycle),
                self.tracking.copy() if self.tracking else None,
                self.status,
                self.error_output,
                self.warnings,
                self.exec_error)

    def add_state_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY, notify_on_register=False):
        with self._state_lock:
            self._state_notification.add_observer(observer, priority)
            if notify_on_register:
                self._state_notification.notify_phase_changed(observer, self.create_snapshot())

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
        self._coord.release()

    def stop(self):
        """
        Cancel not yet started execution or stop started execution.
        Due to synchronous design there is a small window when an execution can be stopped before it is started.
        All execution implementations must cope with such scenario.
        """
        self._phaser.stop()

    def interrupted(self):
        """
        Cancel not yet started execution or interrupt started execution.
        Due to synchronous design there is a small window when an execution can be interrupted before it is started.
        All execution implementations must cope with such scenario.
        """
        self._phaser.stop()  # TODO Interrupt

    def add_warning(self, warning):
        self._warnings.update([warning.name])
        log.warning(self._log('new_warning', 'warning=[{}]', warning))
        self._warning_notification.notify_all(self.create_snapshot(),
                                              WarnEventCtx(warning, self._warnings[warning.name]))

    def run(self):
        # Forward output from execution to the job instance for the instance's output listeners
        self._execution.add_output_observer(self)

        try:
            self._phaser.run()
        finally:
            self._execution.remove_output_observer(self)

    def _phase_change(self, prev_phase, new_phase, ordinal):
        if self._phaser.run_error:
            if self._phaser.termination_status == TerminationStatus.ERROR:
                log.exception(self._log('unexpected_error', "reason=[{}]", self._phaser.run_error))
            else:
                log.warning(self._log('job_not_completed', "reason=[{}]", self._phaser.run_error))

        level = logging.WARN if self._phaser.termination_status.has_flag(Flag.NONSUCCESS) else logging.INFO
        log.log(level, self._log('phase_changed', "prev_phase=[{}] new_phase=[{}] ordinal=[{}]",
                                 prev_phase.name, new_phase.name, ordinal))

        self._state_notification.notify_all_phase_changed(self.create_snapshot())

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
