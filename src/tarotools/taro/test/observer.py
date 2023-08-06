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

from tarotools.taro.jobs.execution import ExecutionState, ExecutionError, ExecutionPhase
from tarotools.taro.jobs.inst import JobInst, Warn, WarningObserver, JobOutputObserver, WarnEventCtx
from tarotools.taro.jobs.runner import ExecutionStateObserver

log = logging.getLogger(__name__)

type_id = 'test'


class TestStateObserver(ExecutionStateObserver):
    __test__ = False  # To tell pytest it isn't a test class

    def __init__(self):
        self._events: List[Tuple[datetime, JobInst, ExecutionState, ExecutionError]] = []
        self.completion_lock = Condition()

    def state_update(self, job_inst: JobInst, previous_state, new_state, changed):
        self._events.append((datetime.now(), job_inst, new_state, job_inst.exec_error))
        log.info("event=[state_changed] job_info=[{}]".format(job_inst))
        self._release_state_waiter()

    def last_job(self) -> JobInst:
        """
        :return: job of the last event
        """
        return self._events[-1][1]

    def last_state(self, job_id) -> ExecutionState:
        """
        :return: last state of the specified job
        """
        return next(e[2] for e in reversed(self._events) if e[1].job_id == job_id)

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
        terminal_condition = lambda: any((e for e in self._events if e[2].in_phase(ExecutionPhase.TERMINAL)))
        return self._wait_for_state_condition(terminal_condition, timeout)

    def _wait_for_state_condition(self, state_condition: Callable[[], bool], timeout: float):
        with self.completion_lock:
            return self.completion_lock.wait_for(state_condition, timeout)


class TestWarnObserver(WarningObserver):

    def __init__(self):
        self.events: List[Tuple[JobInst, Warn, WarnEventCtx]] = []

    def new_warning(self, job_info: JobInst, warning: Warn, event_ctx: WarnEventCtx):
        self.events.append((job_info, warning, event_ctx))

    def is_empty(self):
        return not self.events


class TestJobOutputObserver(JobOutputObserver):

    def __init__(self):
        self.output = []

    def job_output_update(self, job_info: JobInst, output, is_error):
        self.output.append((job_info, output,))

    def last_output(self):
        return self.output[-1][1]
