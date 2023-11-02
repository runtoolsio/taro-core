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

import logging
from collections import deque, Counter
from typing import List, Tuple, Optional

from tarotools.taro import util
from tarotools.taro.jobs.instance import JobInst, WarnEventCtx, JobInstanceID, JobInstanceMetadata, RunnableJobInstance
from tarotools.taro.run import Phaser, Lifecycle, Flag, Fault
from tarotools.taro.status import StatusObserver
from tarotools.taro.util.observer import DEFAULT_OBSERVER_PRIORITY, CallableNotification, ObservableNotification

log = logging.getLogger(__name__)


def log_observer_error(observer, args, exc):
    log.error("event=[observer_error] observer=[%s], args=[%s] error_type=[%s], error=[%s]", observer, args, exc)


_state_observers = CallableNotification(error_hook=log_observer_error)
_output_observers = CallableNotification(error_hook=log_observer_error)
_warning_observers = CallableNotification(error_hook=log_observer_error)


class RunnerJobInstance(RunnableJobInstance):

    def __init__(self, job_id, phase_steps, *, instance_id=None, **user_params):
        self._id = JobInstanceID(job_id, instance_id or util.unique_timestamp_hex())
        self._phases = phase_steps
        self._phaser = Phaser(Lifecycle(), phase_steps)
        parameters = ()  # TODO
        self._metadata = JobInstanceMetadata(self._id, parameters, user_params)
        self._tracking = None  # TODO The output fields below will be moved to the tracker
        self._last_output = deque(maxlen=10)  # TODO Max len configurable
        self._error_output = deque(maxlen=1000)  # TODO Max len configurable
        self._executing = False
        self._warnings = Counter()
        self._transition_notification = CallableNotification(error_hook=log_observer_error)
        self._warning_notification = CallableNotification(error_hook=log_observer_error)
        self._status_notification = ObservableNotification[StatusObserver](error_hook=log_observer_error)

        self._phaser.transition_hook = self._transition_hook

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
        return self._phaser.create_run_snapshot().lifecycle

    @property
    def tracking(self):
        return self._tracking

    @property
    def status(self):
        return self._tracking.status

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
    def run_error(self) -> Optional[Fault]:
        return self._phaser.create_run_snapshot().run_error

    def create_snapshot(self) -> JobInst:
        run_snapshot = self._phaser.create_run_snapshot()
        return JobInst(
            self.metadata,
            run_snapshot.lifecycle,
            self.tracking.copy(),
            self.status,
            self.error_output,
            self.warnings,
            run_snapshot.run_error)

    def add_transition_callback(self, callback, priority=DEFAULT_OBSERVER_PRIORITY, notify_on_register=False):
        if notify_on_register:
            def add_and_notify_callback(*args):
                self._transition_notification.add_observer(callback)
                callback(self._phaser.create_run_snapshot(), *args)

            self._phaser.execute_last_transition_hook(add_and_notify_callback)
        else:
            self._transition_notification.add_observer(callback)

    def remove_transition_callback(self, callback):
        self._transition_notification.remove_observer(callback)

    def _transition_hook(self, prev_phase, new_phase, ordinal):
        """Executed under phaser transition lock"""
        snapshot = self.create_snapshot()

        if snapshot.run_error:
            log.error(self._log('unexpected_error', "error_type=[{}] reason=[{}]",
                                snapshot.run_error.fault_type, snapshot.run_error.reason))
        elif snapshot.run_failure:
            log.warning(self._log('run_failed', "error_type=[{}] reason=[{}]",
                                  snapshot.run_error.fault_type, snapshot.run_error.reason))

        level = logging.WARN if snapshot.termination_status.has_flag(Flag.NONSUCCESS) else logging.INFO
        log.log(level, self._log('phase_changed', "prev_phase=[{}] new_phase=[{}] ordinal=[{}]",
                                 prev_phase.name, new_phase.name, ordinal))

        self._transition_notification(snapshot, prev_phase, new_phase, ordinal)

    def add_warning_callback(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self._warning_notification.add_observer(observer, priority)

    def remove_warning_callback(self, observer):
        self._warning_notification.remove_observer(observer)

    def add_status_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self._status_notification.add_observer(observer, priority)

    def remove_status_observer(self, observer):
        self._status_notification.remove_observer(observer)

    def run(self):
        for phase_step in self._phaser.steps:
            phase_step.add_status_observer(self._status_notification.observer_proxy)

        try:
            self._phaser.run()
        finally:
            for phase_step in self._phaser.steps:
                phase_step.remove_status_observer(self._status_notification.observer_proxy)

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
        self._warning_notification(self.create_snapshot(), WarnEventCtx(warning, self._warnings[warning.name]))


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
