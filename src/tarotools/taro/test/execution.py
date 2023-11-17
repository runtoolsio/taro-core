"""
:class:`Execution` implementation for testing purposes.

Default :class:`ExecutionState` returned after the execution is COMPLETED.
Different state can be returned by setting after_exec_state: :func:`TestExecution.after_exec_state`.
Alternatively the execution can raise an exception set to raise_exc: :func:`TestExecution.raise_exception`.
"""

import logging
from datetime import datetime, timedelta
from threading import Event
from time import sleep
from typing import List

from tarotools.taro.err import InvalidStateError
from tarotools.taro.execution import OutputExecution
from tarotools.taro.run import TerminationStatus
from tarotools.taro.util import utc_now

log = logging.getLogger(__name__)


class TestExecution(OutputExecution):
    __test__ = False  # To tell pytest it isn't a test class

    def __init__(self, after_exec_state: TerminationStatus = None, raise_exc: Exception = None, *, wait: bool = False):
        if after_exec_state and raise_exc:
            raise ValueError("either after_exec_state or throw_exc must be set", after_exec_state, raise_exc)
        self._term_status = after_exec_state or (None if raise_exc else TerminationStatus.COMPLETED)
        self._raise_exc = raise_exc
        self._wait = Event() if wait else None
        self._execution_occurrences: List[datetime] = []
        self._tracking = None

    def __repr__(self):
        return "{}(ExecutionState.{}, {!r})".format(
            self.__class__.__name__, self._term_status.name, self._raise_exc)

    def after_exec_state(self, state: TerminationStatus):
        self._term_status = state
        self._raise_exc = None
        return self

    def raise_exception(self, exc: Exception):
        self._term_status = None
        self._raise_exc = exc
        return self

    def release(self):
        if self._wait:
            self._wait.set()
        else:
            raise InvalidStateError('Wait not set')

    def execute(self) -> TerminationStatus:
        self._execution_occurrences.append(datetime.now())
        if self._wait:
            self._wait.wait(5)
        if self._term_status:
            log.info('event=[executed] new_state=[%s]', self._term_status.name)
            return self._term_status
        else:
            log.info('event=[executed] exception_raised=[%s]', self._raise_exc)
            raise self._raise_exc

    def executed_count(self):
        return len(self._execution_occurrences)

    def last_execution_occurrence(self) -> datetime:
        return self._execution_occurrences[-1]

    @property
    def tracking(self):
        return self._tracking

    @tracking.setter
    def tracking(self, tracking):
        self._tracking = tracking

    @property
    def status(self):
        return None

    @property
    def parameters(self):
        return ('execution', 'test'),

    def stop(self):
        raise NotImplementedError()

    def interrupted(self):
        raise NotImplementedError()

    def add_output_callback(self, callback):
        pass

    def remove_output_callback(self, callback):
        pass


def lc_active(active_state: TerminationStatus, delta=0):
    sleep(0.001)  # Ensure when executed sequentially the states are chronological
    assert not active_state.in_phase(InstancePhase.TERMINAL), "The state must not be terminal"
    start_date = utc_now() + timedelta(minutes=delta)

    return InstanceLifecycle(
        (InstancePhase.CREATED, start_date),
        (active_state, start_date + timedelta(minutes=delta) + timedelta(seconds=1)))


def lc_ended(termination_status: TerminationStatus, *, start_date=None, end_date=None, delta=0, term_delta=0):
    assert termination_status.in_phase(InstancePhase.TERMINAL), "The state must be terminal"

    if not start_date and not end_date:
        sleep(0.001)  # Ensure when executed sequentially the states are chronological
    start_date = (start_date or utc_now()) + timedelta(minutes=delta)
    end_date = (end_date or (start_date + timedelta(seconds=2))) + timedelta(minutes=term_delta + delta)

    return InstanceLifecycle(
        (InstancePhase.CREATED, start_date),
        (InstancePhase.EXECUTING, start_date + timedelta(minutes=delta) + timedelta(seconds=1)),
        (InstancePhase.TERMINAL, end_date),
        termination_status=termination_status
    )


def lc_pending(*, delta=0):
    return lc_active(TerminationStatus.PENDING, delta=delta)


def lc_queued(*, delta=0):
    return lc_active(TerminationStatus.QUEUED, delta=delta)


def lc_running():
    return lc_active(TerminationStatus.RUNNING)


def lc_completed(*, start_date=None, end_date=None, delta=0, term_delta=0):
    return lc_ended(TerminationStatus.COMPLETED, start_date=start_date, end_date=end_date, delta=delta,
                    term_delta=term_delta)


def lc_failed(*, start_date=None, end_date=None, delta=0, term_delta=0):
    return lc_ended(TerminationStatus.FAILED, start_date=start_date, end_date=end_date, delta=delta, term_delta=term_delta)


def lc_stopped(*, start_date=None, end_date=None, delta=0, term_delta=0):
    return lc_ended(TerminationStatus.STOPPED, start_date=start_date, end_date=end_date, delta=delta,
                    term_delta=term_delta)
