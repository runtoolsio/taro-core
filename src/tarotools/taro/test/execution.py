"""
:class:`Execution` implementation for testing purposes.

Default :class:`ExecutionState` returned after the execution is COMPLETED.
Different state can be returned by setting after_exec_state: :func:`TestExecution.after_exec_state`.
Alternatively the execution can raise an exception set to raise_exc: :func:`TestExecution.raise_exception`.
"""

import logging
from threading import Event
from typing import Type

from tarotools.taro.err import InvalidStateError
from tarotools.taro.execution import OutputExecution, ExecutionResult
from tarotools.taro.util.observer import CallableNotification

log = logging.getLogger(__name__)


class TestExecution(OutputExecution):
    __test__ = False  # To tell pytest it isn't a test class

    def __init__(self,
                 exec_res: ExecutionResult = ExecutionResult.DONE,
                 raise_exc: Type[Exception] = None,
                 *, wait: bool = False, output_text=None):
        self.exec_res = exec_res
        self.raise_exc = raise_exc
        self.output_text = output_text
        self.executed_latch = Event()
        self.output_notification = CallableNotification()
        self._wait = Event() if wait else None

    @property
    def parameters(self):
        return ('execution', 'test'),

    def __repr__(self):
        return "{}(ExecutionState.{}, {!r})".format(self.__class__.__name__, self.exec_res, self.raise_exc)

    def release(self):
        if self._wait:
            self._wait.set()
        else:
            raise InvalidStateError('Wait not set')

    def execute(self) -> ExecutionResult:
        if self.output_text:
            self.output_notification(*self.output_text)
        self.executed_latch.set()
        if self._wait:
            self._wait.wait(2)
        if self.raise_exc:
            log.info('event=[executed] raised_exception=[%s]', self.raise_exc)
            raise self.raise_exc()
        else:
            log.info('event=[executed] result=[%s]', self.exec_res)
            return self.exec_res

    def stop(self):
        if self._wait:
            self._wait.set()

    def interrupted(self):
        raise NotImplementedError()

    def add_callback_output(self, callback):
        self.output_notification.add_observer(callback)

    def remove_callback_output(self, callback):
        self.output_notification.remove_observer(callback)
