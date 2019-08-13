"""
:class:`Execution` implementation for testing purposes.

Default :class:`ExecutionState` returned after the execution is COMPLETED.
Different state can be returned by setting after_exec_state: :func:`TestExecution.after_exec_state`.
Alternatively the execution can raise an exception set to raise_exc: :func:`TestExecution.raise_exception`.
"""

import logging
from taro.execution import Execution, ExecutionState

log = logging.getLogger(__name__)


class TestExecution(Execution):

    def __init__(self, after_exec_state: ExecutionState = None, raise_exc: Exception = None):
        if after_exec_state and raise_exc:
            raise ValueError("either after_exec_state or throw_exc must be set", after_exec_state, raise_exc)
        self._after_exec_state = after_exec_state or (None if raise_exc else ExecutionState.COMPLETED)
        self._raise_exc = raise_exc

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

    def execute(self):
        if self._after_exec_state:
            log.info('event=[executed] new_state=[{}]', self._after_exec_state)
            return self._after_exec_state
        else:
            log.info('event=[executed] exception_raised=[{}]', self._raise_exc)
            raise self._raise_exc
