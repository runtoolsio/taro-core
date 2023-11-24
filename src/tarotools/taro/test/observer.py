"""
:class:`InstanceStateObserver` implementation for testing purposes.

Observed notifications are stored as events in indexed sequence.
The wait_for_* methods allow to wait for a specific event to be observed. This can be used for synchronization
between job executing thread and testing thread.
"""

import logging
from queue import Queue
from threading import Condition
from typing import Tuple, List, Callable

from tarotools.taro.jobs.instance import JobRun, InstanceTransitionObserver, \
    InstanceOutputObserver
from tarotools.taro.run import PhaseRun, RunState, PhaseMetadata

log = logging.getLogger(__name__)

type_id = 'test'


class GenericObserver:

    def __init__(self):
        self.updates = Queue()

    def __getattr__(self, name):
        def method(*args):
            self.updates.put_nowait((name, args))

        return method

    def __call__(self, *args):
        self.updates.put_nowait(("__call__", args))


class TestTransitionObserver(InstanceTransitionObserver):
    __test__ = False  # To tell pytest it isn't a test class

    def __init__(self):
        self._events: List[Tuple[JobRun, PhaseRun, PhaseRun, int]] = []
        self.completion_lock = Condition()

    def new_instance_phase(self, job_run: JobRun, previous_phase, new_phase, ordinal):
        self._events.append((job_run, previous_phase, new_phase, ordinal))
        log.info("event=[state_changed] job_info=[{}]".format(job_run))
        self._release_state_waiter()

    def last_job(self) -> JobRun:
        """
        :return: job of the last event
        """
        return self._events[-1][0]

    @property
    def last_state(self):
        return self._events[-1][2].run_state

    def last_job_state(self, job_id):
        """
        :return: last state of the specified job
        """
        return next(e[2].run_state for e in reversed(self._events) if e[0].job_id == job_id)

    def exec_state(self, event_idx: int):
        """
        :param event_idx: event index
        :return: execution state of the event on given index
        """
        return self._events[event_idx][2].run_state

    def _release_state_waiter(self):
        with self.completion_lock:
            self.completion_lock.notify()  # Support only one-to-one thread sync to keep things simple

    def wait_for_state(self, exec_state, timeout: float = 1) -> bool:
        """
        Wait for receiving notification with the specified state

        :param exec_state: Waits for the state specified by this parameter
        :param timeout: Waiting interval in seconds
        :return: True when specified state received False when timed out
        """
        return self._wait_for_state_condition(lambda: exec_state in (e[2].run_state for e in self._events), timeout)

    def wait_for_ended_state(self, timeout: float = 1) -> bool:
        """
        Wait for receiving notification with a terminal state

        :param timeout: Waiting interval in seconds
        :return: True when terminal state received False when timed out
        """
        terminal_condition = lambda: any((e for e in self._events if e[2].run_state == RunState.ENDED))
        return self._wait_for_state_condition(terminal_condition, timeout)

    def _wait_for_state_condition(self, state_condition: Callable[[], bool], timeout: float):
        with self.completion_lock:
            return self.completion_lock.wait_for(state_condition, timeout)


class TestOutputObserver(InstanceOutputObserver):
    __test__ = False  # To tell pytest it isn't a test class

    def __init__(self):
        self.outputs = []

    def new_instance_output(self, job_run: JobRun, phase_meta: PhaseMetadata, output: str, is_error: bool):
        self.outputs.append((job_run, phase_meta, output, is_error))

    @property
    def last_line(self):
        return self.outputs[-1][2]
