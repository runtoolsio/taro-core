"""
:class:`ExecutionStateObserver` implementation for testing purposes.

Observed notifications are stored as events in indexed sequence.
The wait_for_* methods allow to wait for a specific event to be observed. This can be used for synchronization
between job executing thread and testing thread.
"""

import logging
from datetime import datetime
from threading import Condition
from typing import Tuple, List, Callable

from taro.execution import ExecutionState, ExecutionError
from taro.job import JobInfo
from taro.runner import ExecutionStateObserver

log = logging.getLogger(__name__)

type_id = 'test'


class TestObserver(ExecutionStateObserver):
    __test__ = False  # To tell pytest it isn't a test class

    def __init__(self):
        self._events: List[Tuple[datetime, JobInfo, ExecutionState, ExecutionError]] = []
        self.completion_lock = Condition()

    def state_update(self, job_info: JobInfo):
        self._events.append((datetime.now(), job_info, job_info.state, job_info.exec_error))
        log.info("event=[state_changed] job_info=[{}]".format(job_info))
        self._release_state_waiter()

    def last_job(self) -> JobInfo:
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

    def exec_error(self, event_idx: int) -> ExecutionError:
        """
        :param event_idx: event index
        :return: execution state of the event on given index
        """
        return self._events[event_idx][3]

    def _release_state_waiter(self):
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
        with self.completion_lock:
            return self.completion_lock.wait_for(state_condition, timeout)
