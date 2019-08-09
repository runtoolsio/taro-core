"""
ExecutionStateObserver implementation for testing purposes.

Observed notifications are stored as events in indexed sequence.
The wait_for_* methods allow to wait for a specific event to be observed. This can be used for synchronization
between job executing thread and testing thread.
"""

import logging
from datetime import datetime
from threading import Condition
from typing import Tuple, List, Callable

from taro.execution import ExecutionState, ExecutionError
from taro.job import Job
from taro.runner import ExecutionStateObserver

log = logging.getLogger(__name__)

type_id = 'test'


class TestObserver(ExecutionStateObserver):

    def __init__(self, support_waiter: bool = False):
        self._events: List[Tuple[datetime, Job, ExecutionState, ExecutionError]] = []
        self._support_waiter = support_waiter
        if self._support_waiter:
            self.completion_lock = Condition()

    def notify(self, job: Job, exec_state: ExecutionState, exec_error=None):
        self._events.append((datetime.now(), job, exec_state, exec_error))

        log_msg = "event=[notification] job=[{}] state=[{}]".format(job, exec_state)
        if exec_error:
            log_msg += " error=[{}]".format(exec_error)
        log.info(log_msg)

        self._release_state_waiter()

    def last_job(self) -> Job:
        """
        :return: job of the last event
        """
        return self._events[-1][1]

    def exec_state(self, event_idx: int) -> ExecutionState:
        """
        :param event_idx: event index
        :return: execution state of the event on given index
        """
        return self._events[event_idx][2]

    def _release_state_waiter(self):
        if self._support_waiter:
            with self.completion_lock:
                self.completion_lock.notify()  # Support only one-to-one thread sync to keep things simple

    def wait_for_state(self, exec_state: ExecutionState, timeout: float = 1) -> bool:
        """
        Wait for receiving notification with the specified state

        :param exec_state: Waits for the state specified by this parameter
        :param timeout: Waiting interval in seconds
        :return: True when specified state received False when timed out
        """
        return self._wait_for_state_condition(lambda: exec_state in (e[2] for e in self._events), timeout)

    def wait_for_terminal_state(self, timeout: float = 1) -> bool:
        """
        Wait for receiving notification with a terminal state

        :param timeout: Waiting interval in seconds
        :return: True when terminal state received False when timed out
        """
        return self._wait_for_state_condition(lambda: any((e for e in self._events if e[2].is_terminal())), timeout)

    def _wait_for_state_condition(self, state_condition: Callable[[], bool], timeout: float):
        if not self._support_waiter:
            raise ValueError("support_waiter set to false")

        with self.completion_lock:
            return self.completion_lock.wait_for(state_condition, timeout)
