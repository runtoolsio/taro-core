import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from threading import Condition, Event, Lock

from tarotools import taro
from tarotools.taro.jobs import lock
from tarotools.taro.jobs.criteria import JobRunIdCriterion, TerminationCriterion, JobRunAggregatedCriteria
from tarotools.taro.jobs.instance import JobRuns, InstanceTransitionObserver, JobRun
from tarotools.taro.listening import InstanceTransitionReceiver
from tarotools.taro.run import RunState, Phase, TerminationStatus, PhaseRun

log = logging.getLogger(__name__)


class ApprovalPhase(Phase):
    """
    Approval parameters (incl. timeout) + approval eval as separate objects
    TODO: parameters
    """

    def __init__(self, phase_name, timeout=0):
        super().__init__(phase_name, RunState.PENDING)
        self._timeout = timeout
        self._event = Event()

    def run(self) -> TerminationStatus:
        resolved = self._event.wait(self._timeout or None)
        if resolved:
            return TerminationStatus.NONE
        else:
            return TerminationStatus.TIMEOUT

    def approve(self):
        self._event.set()

    def is_approved(self):
        self._event.is_set()

    def stop(self):
        self._event.set()

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED


class NoOverlapPhase(Phase):
    """
    TODO Global lock
    """
    def __init__(self, phase_name, no_overlap_id, until_phase=None):
        if not no_overlap_id:
            raise ValueError("no_overlap_id cannot be empty")

        params = {
            'phase': 'no_overlap',
            'no_overlap_id': no_overlap_id,
            'until_phase': until_phase
        }
        super().__init__(phase_name, RunState.EVALUATING, params)
        self._no_overlap_id = no_overlap_id

    def run(self):
        # TODO Phase params criteria
        runs, _ = taro.client.get_active_runs()
        if any(r for r in runs if self._in_no_overlap_phase(r)):
            return TerminationStatus.INVALID_OVERLAP

        return TerminationStatus.NONE

    def _in_no_overlap_phase(self, job_run):
        no_overlap_phase = None
        until_phase = None

        for idx, phase_meta in enumerate(job_run.phases):
            if phase_meta.parameters.get('no_overlap_id') == self._no_overlap_id:
                if (idx + 1) >= len(job_run.phases):
                    continue  # No next phase - shouldn't happen but check anyway

                no_overlap_phase = phase_meta.phase.name
                until_phase = phase_meta.parameters.get('until_phase') or job_run.phases[idx + 1].phase.name

        if not no_overlap_phase:
            return False

        protected_phases = job_run.lifecycle.phases_between(no_overlap_phase, until_phase)
        return job_run.lifecycle.phase in protected_phases

    def stop(self):
        pass

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED


class DependencyPhase(Phase):

    def __init__(self, phase_name, dependency_match):
        self._parameters = {'phase': 'dependency', 'dependency': str(dependency_match.serialize(False))}
        super().__init__(phase_name, RunState.EVALUATING, self._parameters)
        self._dependency_match = dependency_match

    @property
    def dependency_match(self):
        return self._dependency_match

    @property
    def parameters(self):
        return self._parameters

    def run(self):
        instances, _ = taro.client.get_active_runs()
        if not any(i for i in instances if self._dependency_match.matches(i)):
            return TerminationStatus.UNSATISFIED

        return TerminationStatus.NONE

    def stop(self):
        pass

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED


class WaitingPhase(Phase):
    """
    """

    def __init__(self, phase_name, observable_conditions, timeout=0):
        super().__init__(phase_name, RunState.WAITING)
        self._observable_conditions = observable_conditions
        self._timeout = timeout
        self._conditions_lock = Lock()
        self._event = Event()
        self._term_status = TerminationStatus.NONE

    def run(self):
        for condition in self._observable_conditions:
            condition.add_result_listener(self._result_observer)
            condition.start_evaluating()

        resolved = self._event.wait(self._timeout or None)
        if not resolved:
            self._term_status = TerminationStatus.TIMEOUT

        self._stop_all()
        return self._term_status

    def _result_observer(self, *_):
        wait = False
        with self._conditions_lock:
            for condition in self._observable_conditions:
                if not condition.result:
                    wait = True
                elif not condition.result.success:
                    self._term_status = TerminationStatus.UNSATISFIED
                    wait = False
                    break

        if not wait:
            self._event.set()

    def stop(self):
        self._stop_all()
        self._event.set()

    @property
    def stop_status(self):
        return TerminationStatus.CANCELLED

    def _stop_all(self):
        for condition in self._observable_conditions:
            condition.stop()


class ConditionResult(Enum):
    """
    Enum representing the result of a condition evaluation.

    Attributes:
        NONE: The condition has not been evaluated yet.
        SATISFIED: The condition is satisfied.
        UNSATISFIED: The condition is not satisfied.
        EVALUATION_ERROR: The condition could not be evaluated due to an error in the evaluation logic.
    """
    NONE = (auto(), False)
    SATISFIED = (auto(), True)
    UNSATISFIED = (auto(), False)
    EVALUATION_ERROR = (auto(), False)

    def __new__(cls, value, success):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.success = success
        return obj

    def __bool__(self):
        return self != ConditionResult.NONE


class ObservableCondition(ABC):
    """
    Abstract base class representing a (child) waiter associated with a specific (parent) pending object.

    A waiter is designed to be held by a job instance, enabling the job to enter its waiting phase
    before actual execution. This allows for synchronization between different parts of the system.
    Depending on the parent waiting, the waiter can either be manually released, or all associated
    waiters can be released simultaneously when the main condition of the waiting is met.

    TODO:
    1. Add notifications to this class
    """

    @abstractmethod
    def start_evaluation(self) -> None:
        """
        Instructs the waiter to begin waiting on its associated condition.

        When invoked by a job instance, the job enters its pending phase, potentially waiting for
        the overarching pending condition to be met or for a manual release.
        """
        pass

    @property
    @abstractmethod
    def result(self):
        """
        Returns:
            ConditionResult: The result of the evaluation or NONE if not yet evaluated.
        """
        pass

    @abstractmethod
    def add_result_listener(self, listener):
        pass

    @abstractmethod
    def remove_result_listener(self, listener):
        pass

    def stop(self):
        pass


class Queue:

    @abstractmethod
    def create_waiter(self, job_instance, state_on_dequeue):
        pass


class QueuedState(Enum):
    NONE = auto(), False
    IN_QUEUE = auto(), False
    DISPATCHED = auto(), True
    CANCELLED = auto(), True
    UNKNOWN = auto(), False

    def __new__(cls, value, dequeued):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.dequeued = dequeued
        return obj

    @classmethod
    def from_str(cls, value: str):
        try:
            return cls[value.upper()]
        except KeyError:
            return cls.UNKNOWN


class QueueWaiter:

    @property
    @abstractmethod
    def state(self):
        """
        Returns:
            QueuedState: The current state of the waiter.
        """
        pass

    @abstractmethod
    def wait(self):
        pass

    @abstractmethod
    def cancel(self):
        pass

    @abstractmethod
    def signal_dispatch(self):
        pass


@dataclass
class ExecutionGroupLimit:
    group: str
    max_executions: int


class ExecutionQueue(Queue, InstanceTransitionObserver):

    def __init__(self, queue_id, max_executions, queue_locker=lock.default_queue_locker(),
                 state_receiver_factory=InstanceTransitionReceiver):
        super().__init__("QUEUE", f"{queue_id}<={max_executions}")
        if not queue_id:
            raise ValueError('Queue ID must be specified')
        if max_executions < 1:
            raise ValueError('Max executions must be greater than zero')
        self._queue_id = queue_id
        self._max_executions = max_executions
        self._locker = queue_locker
        self._state_receiver_factory = state_receiver_factory

        self._wait_guard = Condition()
        # vv Guarding these fields vv
        self._wait_counter = 0
        self._current_wait = False
        self._state_receiver = None

        self._parameters = (
            ('coord', 'execution_queue'),
            ('execution_group', queue_id),
            ('max_executions', max_executions)
        )

    def create_waiter(self, instance, dequeue_state_resolver):
        return self._Waiter(self, dequeue_state_resolver)

    class _Waiter(QueueWaiter):

        def __init__(self, queue: "ExecutionQueue", dispatch_status_resolver):
            self.queue = queue
            self.dispatch_status_resolver = dispatch_status_resolver
            self.term_status = None
            self._state = QueuedState.NONE

        @property
        def state(self):
            return self._state

        def wait(self):
            while True:
                with self.queue._wait_guard:
                    if self._state == QueuedState.NONE:
                        # Should new waiter run scheduler?
                        self._state = QueuedState.IN_QUEUE

                    if self._state.dequeued:
                        return self.term_status

                    if self.queue._current_wait:
                        self.queue._wait_guard.wait()
                        continue

                    self.queue._current_wait = True
                    self.queue._start_listening()

                with self.queue._locker():
                    self.queue._dispatch_next()

                continue

        def cancel(self):
            with self.queue._wait_guard:
                if self._state.dequeued:
                    return

                self._state = QueuedState.CANCELLED
                self.queue._wait_guard.notify_all()

        def signal_dispatch(self):
            with self.queue._wait_guard:
                if self._state.dequeued:
                    return False  # Cancelled

                self._state = QueuedState.DISPATCHED
                self.term_status = self.dispatch_status_resolver()
                self.queue._wait_guard.notify_all()

            return not bool(self.term_status)

    def _start_listening(self):
        self._state_receiver = self._state_receiver_factory()
        self._state_receiver.listeners.append(self)
        self._state_receiver.start()

    def _dispatch_next(self):
        criteria = JobRunAggregatedCriteria(
            state_criteria=TerminationCriterion(phases={Phase.QUEUED, Phase.EXECUTING}),
            param_sets=set(self._parameters)
        )
        jobs, _ = taro.client.get_active_runs(criteria)

        group_jobs_sorted = JobRuns(sorted(jobs, key=RunState.CREATED))
        next_count = self._max_executions - len(group_jobs_sorted.executing)
        if next_count <= 0:
            return False

        for next_proceed in group_jobs_sorted.queued:
            # TODO Use identity ID
            signal_resp = taro.client.signal_dispatch(JobRunAggregatedCriteria(JobRunIdCriterion.for_instance(next_proceed)))
            for r in signal_resp.responses:
                if r.executed:
                    next_count -= 1
                    if next_count <= 0:
                        return

    def new_phase(self, job_run: JobRun, previous_phase: PhaseRun, new_phase: PhaseRun, ordinal: int):
        with self._wait_guard:
            if not self._current_wait:
                return
            if new_phase.run_state == RunState.ENDED and job_run.metadata.contains_system_parameters(self._parameters):
                self._current_wait = False
                self._stop_listening()
                self._wait_guard.notify()

    def _stop_listening(self):
        self._state_receiver.close()
        self._state_receiver.listeners.remove(self)
        self._state_receiver = None
