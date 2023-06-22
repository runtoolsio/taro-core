"""
:class:`Execution` implementation for testing purposes.

Default :class:`ExecutionState` returned after the execution is COMPLETED.
Different state can be returned by setting after_exec_state: :func:`TestExecution.after_exec_state`.
Alternatively the execution can raise an exception set to raise_exc: :func:`TestExecution.raise_exception`.
"""

import logging
from datetime import datetime, timedelta
from threading import Event
from typing import List

from taro import ExecutionLifecycle
from taro.err import InvalidStateError
from taro.jobs.execution import ExecutionState, OutputExecution, ExecutionPhase
from taro.util import utc_now

log = logging.getLogger(__name__)


class TestExecution(OutputExecution):
    __test__ = False  # To tell pytest it isn't a test class

    def __init__(self, after_exec_state: ExecutionState = None, raise_exc: Exception = None, *, wait: bool = False):
        if after_exec_state and raise_exc:
            raise ValueError("either after_exec_state or throw_exc must be set", after_exec_state, raise_exc)
        self._after_exec_state = after_exec_state or (None if raise_exc else ExecutionState.COMPLETED)
        self._raise_exc = raise_exc
        self._wait = Event() if wait else None
        self._execution_occurrences: List[datetime] = []
        self._tracking = None

    def __repr__(self):
        return "{}(ExecutionState.{}, {!r})".format(
            self.__class__.__name__, self._after_exec_state.name, self._raise_exc)

    def after_exec_state(self, state: ExecutionState):
        self._after_exec_state = state
        self._raise_exc = None
        return self

    def raise_exception(self, exc: Exception):
        self._after_exec_state = None
        self._raise_exc = exc
        return self

    @property
    def is_async(self) -> bool:
        return False

    def release(self):
        if self._wait:
            self._wait.set()
        else:
            raise InvalidStateError('Wait not set')

    def execute(self) -> ExecutionState:
        self._execution_occurrences.append(datetime.now())
        if self._wait:
            self._wait.wait(5)
        if self._after_exec_state:
            log.info('event=[executed] new_state=[%s]', self._after_exec_state.name)
            return self._after_exec_state
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

    def add_output_observer(self, observer):
        pass

    def remove_output_observer(self, observer):
        pass


def lc_active(active_state: ExecutionState, delta=0):
    assert not active_state.in_phase(ExecutionPhase.TERMINAL), "The state must not be terminal"
    start_date = utc_now() + timedelta(minutes=delta)

    return ExecutionLifecycle(
        (ExecutionState.CREATED, start_date),
        (active_state, start_date + timedelta(minutes=delta + 1)))


def lc_ended(terminal_state: ExecutionState, *, start_date=None, end_date=None, delta=0, term_delta=0):
    assert terminal_state.in_phase(ExecutionPhase.TERMINAL), "The state must be terminal"
    start_date = start_date or (utc_now() + timedelta(minutes=delta))
    end_date = end_date or (start_date + timedelta(minutes=term_delta + delta + 2))

    return ExecutionLifecycle(
        (ExecutionState.CREATED, start_date),
        (ExecutionState.RUNNING, start_date + timedelta(minutes=delta + 1)),
        (terminal_state, end_date)
    )


def lc_pending(*, delta=0):
    return lc_active(ExecutionState.PENDING, delta=delta)


def lc_running():
    return lc_active(ExecutionState.RUNNING)


def lc_completed(*, start_date=None, end_date=None, delta=0, term_delta=0):
    return lc_ended(ExecutionState.COMPLETED, start_date=start_date, end_date=end_date, delta=delta, term_delta=term_delta)


def lc_failed(*, start_date=None, end_date=None, delta=0, term_delta=0):
    return lc_ended(ExecutionState.FAILED, start_date=start_date, end_date=end_date, delta=delta, term_delta=term_delta)


def lc_stopped(*, start_date=None, end_date=None, delta=0, term_delta=0):
    return lc_ended(ExecutionState.STOPPED, start_date=start_date, end_date=end_date, delta=delta, term_delta=term_delta)
