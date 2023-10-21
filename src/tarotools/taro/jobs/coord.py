import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from threading import Condition
from typing import Sequence, Optional

from tarotools import taro
from tarotools.taro.jobs import lock
from tarotools.taro.jobs.execution import ExecutionState, ExecutionPhase, Flag, Phase
from tarotools.taro.jobs.inst import JobInstances, InstanceMatchCriteria, IDMatchCriteria, StateCriteria
from tarotools.taro.listening import StateReceiver, ExecutionStateEventObserver
from tarotools.taro.log import timing

log = logging.getLogger(__name__)


@dataclass
class Identifier:
    """
    Represents a unique identifier for a coordination component.

    Attributes:
        type_ (str): Specifies the type of the coordination object.
        id (str): A value for distinguishing between objects of the same type.
    """
    type_: str
    id: str


class Identifiable(ABC):

    def __init__(self, type_, id_):
        self._type = type_
        self._id = id_

    @property
    def identifier(self):
        """
        Returns the unique identifier associated with this coordination object.

        Returns:
            Identifier: The identifier of the this object.
        """
        return Identifier(type_=self._type, id=self._id)


class Pending(Identifiable):
    """
    Abstract base class representing a generic pending condition.

    A "pending" allows job instances to enter a pending phase before they actually start executing.
    Derived classes should provide specific implementations of the pending behavior.

    Pending either encapsulates a WAITING condition and automatically signals all associated waiters
    when this condition is SATISFIED, or it expects manual action for RELEASE when an external condition is met.
    This manual release can target all waiters simultaneously or handle each waiter individually.
    """

    @abstractmethod
    def create_waiter(self, job_instance):
        """
        Creates and returns a new waiter associated with this pending object and the provided job instance.

        A waiter allows job instances to enter their pending phase, where they might wait for a specific condition
        encapsulated by the pending object. Multiple waiters (job instances) can be associated with a single pending,
        allowing multiple job instances to await their specific conditions concurrently. Each waiter is tied
        to a specific job instance. All waiters associated with a pending can be released simultaneously
        when the main condition of the pending is met, or each waiter can be manually released individually.

        Parameters:
            job_instance: The job instance that will hold and utilize the created waiter.

        Returns:
            PendingWaiter: A new waiter object designed to be held by the provided job instance.
        """
        pass

    @abstractmethod
    def release_all(self) -> None:
        """
        Releases all waiters associated with this pending object.
        """
        pass


class PendingWaiterState(Enum):
    """
    Enum representing the various states a waiter can be in.

    Attributes:
        WAITING: The waiter is actively waiting for its associated condition or to be manually released.
        RELEASED: The waiter has been manually released, either as an expected action or to override the wait condition.
        SATISFIED: The main condition the waiter was waiting for has been met.
    """
    WAITING = auto()
    RELEASED = auto()
    SATISFIED = auto()


class PendingWaiter(ABC):
    """
    Abstract base class representing a (child) waiter associated with a specific (parent) pending object.

    A waiter is designed to be held by a job instance, enabling the job to enter its pending phase
    before actual execution. This allows for synchronization between different parts of the system.
    Depending on the parent pending, the waiter can either be manually released, or all associated
    waiters can be released simultaneously when the main condition of the pending is met.
    """

    @property
    @abstractmethod
    def parent_pending(self):
        """
        Returns the parent pending object with which this waiter is associated.

        The parent pending object encapsulates the specific condition or conditions that job instances,
        holding this waiter, might await before starting their execution.

        Returns:
            Pending: The associated parent pending object.
        """
        pass

    @property
    @abstractmethod
    def state(self):
        """
        Returns:
            PendingWaiterState: The current state of the waiter.
        """
        pass

    @abstractmethod
    def wait(self) -> None:
        """
        Instructs the waiter to begin waiting on its associated condition.

        When invoked by a job instance, the job enters its pending phase, potentially waiting for
        the overarching pending condition to be met or for a manual release.
        """
        pass

    @abstractmethod
    def release(self) -> None:
        """
        Manually releases the waiter, even if the overarching pending condition is not met.
        """
        pass

    # TODO unblock method


class Latch(Pending):

    def __init__(self, latch_id):
        super().__init__("LATCH", latch_id)
        self._condition = Condition()
        self._released = False

    def create_waiter(self, instance) -> "PendingWaiter":
        return self._Waiter(latch=self)

    def release_all(self):
        with self._condition:
            self._released = True
            self._condition.notify_all()

    class _Waiter(PendingWaiter):

        def __init__(self, latch: "Latch"):
            self.latch = latch
            self.released = False

        @property
        def parent_pending(self):
            return self.latch

        @property
        def state(self):
            if self.released:
                return PendingWaiterState.RELEASED
            if self.latch._released:
                return PendingWaiterState.SATISFIED

            return PendingWaiterState.WAITING

        def wait(self):
            while True:
                with self.latch._condition:
                    if self.released or self.latch._released:
                        return
                    self.latch._condition.wait()

        def release(self):
            with self.latch._condition:
                self.released = True
                self.latch._condition.notify_all()


class PreExecCondition(Identifiable):
    """
    Represents a pre-executing condition that must be satisfied before a job instance can transition
    to the EXECUTING phase. The condition can be evaluated multiple times. For each evaluation an evaluator
    must be created.
    """

    @abstractmethod
    def create_evaluator(self, job_instance):
        """
        Creates and returns a new evaluator associated with this pre-execution condition and the provided job instance.

        Parameters:
            job_instance: The job instance that will utilize the created evaluator.

        Returns:
            PreExecEvaluator: A new evaluator object designed for the provided job instance.
        """
        pass


class ConditionState(Enum):
    """
    Enum representing the various states an evaluation can be in.

    Attributes:
        NOT_EVALUATED: The condition has not been evaluated yet.
        SATISFIED: The condition is satisfied.
        UNSATISFIED: The condition is not satisfied.
        EVALUATION_ERROR: The condition could not be evaluated due to an error in the evaluation logic.
    """
    NOT_EVALUATED = auto()
    SATISFIED = auto()
    UNSATISFIED = auto()
    EVALUATION_ERROR = auto()


class PreExecEvaluator(ABC):
    """
    Represents an individual evaluation instance for a pre-executing condition. Allows for independent
    assessment of the condition across different job instances to determine if they can proceed to the EXECUTING phase.
    """

    @property
    @abstractmethod
    def state(self):
        """
        Returns:
            ConditionState: The current state of the evaluation.
        """
        pass

    @abstractmethod
    def evaluate(self):
        """
        Evaluates the associated pre-execution condition.

        Returns:
            bool: True if the condition is satisfied, False otherwise.
        """
        pass


class NoOverlap(PreExecCondition):
    """
    TODO Sync
    """

    def __init__(self, instance_match=None):
        no_overlap = str(instance_match.to_dict(False)) if instance_match else 'same_job_id'
        super().__init__("NO_OVERLAP", no_overlap)
        self._instance_match = instance_match
        self._parameters = (('condition', 'no_overlap'), ('no_overlap', no_overlap))

    @property
    def instance_match(self):
        return self._instance_match

    @property
    def parameters(self):
        return self._parameters

    def create_evaluator(self, instance) -> PreExecEvaluator:
        return self._Evaluator(self, instance)

    class _Evaluator(PreExecEvaluator):

        def __init__(self, condition: "NoOverlap", instance):
            self.condition = condition
            self.instance = instance
            self._state = ConditionState.NOT_EVALUATED

        @property
        def state(self):
            return self._state

        def evaluate(self) -> bool:
            inst_match = self.condition._instance_match or InstanceMatchCriteria(IDMatchCriteria(self.instance.job_id))

            instances, _ = taro.client.read_instances()
            if any(i for i in instances if i.id != self.instance.id and inst_match.matches(i)):
                self._state = ConditionState.UNSATISFIED
                return False

            self._state = ConditionState.SATISFIED
            return True


class Dependency(PreExecCondition):

    def __init__(self, dependency_match):
        dependency = str(dependency_match.to_dict(False))
        super().__init__("DEPENDENCY", dependency)
        self._dependency_match = dependency_match
        self._parameters = (
            ('coord', 'dependency'),
            ('coord_type', 'pre_exec_condition'),
            ('dependency', dependency),
        )

    @property
    def dependency_match(self):
        return self._dependency_match

    @property
    def parameters(self):
        return self._parameters

    def create_evaluator(self, instance) -> PreExecEvaluator:
        return self._Evaluator(self)

    class _Evaluator(PreExecEvaluator):

        def __init__(self, dependency: "Dependency"):
            self._dependency = dependency
            self._evaluated_state = ConditionState.NOT_EVALUATED

        def evaluate(self) -> bool:
            instances, _ = taro.client.read_instances()
            if not any(i for i in instances if self._dependency._dependency_match.matches(i)):
                self._evaluated_state = ConditionState.UNSATISFIED
                return False

            self._evaluated_state = ConditionState.SATISFIED
            return True

        @property
        def state(self) -> ConditionState:
            return self._evaluated_state


class Queue(Identifiable):

    @abstractmethod
    def create_waiter(self, job_instance, state_on_dequeue):
        pass


class QueueWaiterState(Enum):
    BEFORE_QUEUING = auto(), False
    QUEUING = auto(), False
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
            QueueWaiterState: The current state of the waiter.
        """
        pass

    @abstractmethod
    def wait(self):
        pass

    @abstractmethod
    def release(self):
        pass

    @abstractmethod
    def signal_proceed(self):
        pass


@dataclass
class ExecutionGroupLimit:
    group: str
    max_executions: int


class ExecutionQueue(Queue, ExecutionStateEventObserver):

    def __init__(self, execution_group, max_executions, queue_locker=lock.default_queue_locker(),
                 state_receiver_factory=StateReceiver):
        super().__init__("QUEUE", f"{execution_group}<={max_executions}")
        if not execution_group:
            raise ValueError('Execution group must be specified')
        if max_executions < 1:
            raise ValueError('Max executions must be greater than zero')
        self._group = execution_group
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
            ('execution_group', execution_group),
            ('max_executions', max_executions)
        )

    def create_waiter(self, instance, dequeue_state_resolver):
        return self._Waiter(self, instance, dequeue_state_resolver)

    class _Waiter(QueueWaiter):

        def __init__(self, queue: "ExecutionQueue", instance, dispatch_state_resolver):
            self.queue = queue
            self.instance = instance
            self.dispatch_state_resolver = dispatch_state_resolver
            self._state = QueueWaiterState.BEFORE_QUEUING
            self.dispatch_exec_state = ExecutionState.NONE

        @property
        def state(self):
            return self._state

        def wait(self):
            while True:
                with self.queue._wait_guard:
                    if self._state == QueueWaiterState.BEFORE_QUEUING:
                        # Should new waiter run scheduler?
                        self._state = QueueWaiterState.QUEUING

                    if self._state.dequeued:
                        return

                    if self.queue._current_wait:
                        self.queue._wait_guard.wait()
                        continue

                    self.queue._current_wait = True
                    self.queue._start_listening()

                with self.queue._locker():
                    self.queue._dispatch_next()

                continue

        def release(self):
            pass

        def signal_proceed(self):
            with self.queue._wait_guard:
                self._state = QueueWaiterState.DISPATCHED
                self.dispatch_exec_state = self.dispatch_state_resolver()
                self.queue._wait_guard.notify_all()

            return self.dispatch_exec_state.in_phase(ExecutionPhase.EXECUTING)

    def _start_listening(self):
        self._state_receiver = self._state_receiver_factory()
        self._state_receiver.listeners.append(self)
        self._state_receiver.start()

    def _dispatch_next(self):
        criteria = InstanceMatchCriteria(
            state_criteria=StateCriteria(phases={Phase.QUEUED, Phase.EXECUTING}),
            param_sets=set(self._parameters)
        )
        jobs, _ = taro.client.read_instances(criteria)

        group_jobs_sorted = JobInstances(sorted(jobs, key=lambda job: job.lifecycle.changed_at(ExecutionState.CREATED)))
        next_count = self._max_executions - len(group_jobs_sorted.executing)
        if next_count <= 0:
            return False

        for next_proceed in group_jobs_sorted.queued:
            # TODO Use identity ID
            signal_resp = taro.client.signal_proceed(InstanceMatchCriteria(IDMatchCriteria.for_instance(next_proceed)))
            for r in signal_resp.responses:
                if r.executed:
                    next_count -= 1
                    if next_count <= 0:
                        return

    def state_update(self, instance_meta, previous_state, new_state, changed):
        with self._wait_guard:
            if not self._current_wait:
                return
            if new_state.in_phase(ExecutionPhase.TERMINAL) and instance_meta.contains_parameters(self._parameters):
                self._current_wait = False
                self._stop_listening()
                self._wait_guard.notify()

    def _in_exec_group(self, instance_meta):
        return instance_meta.id.job_id == self._group or \
            any(1 for name, value in instance_meta.parameters if name == 'execution_group' and value == self._group)

    def _stop_listening(self):
        self._state_receiver.close()
        self._state_receiver.listeners.remove(self)
        self._state_receiver = None


# --------------------- OLD DESIGN BELOW ------------------------ #

class WaitCondition:
    LATCH = "LATCH"
    EXEC_GROUP = "EXEC_GROUP"


class Directive(ABC):
    pass


class Continue(Directive):
    # TBA
    pass


CONTINUE = Continue()


class Wait(Directive):
    def __init__(self, state: ExecutionState, wait_condition: str = None):
        self._wait_condition = wait_condition
        if not state.has_flag(Flag.WAITING):
            raise ValueError("Not a waiting state as expected by wait directive: " + str(state))
        self.state = state

    @property
    def wait_condition(self) -> Optional[str]:
        return self._wait_condition

    @abstractmethod
    def wait(self):
        """To be implemented by subclasses."""
        pass

    @abstractmethod
    def release(self):
        """
        Interrupt waiting
        """
        pass


class Reject(Directive):
    def __init__(self, state: ExecutionState):
        if not state.has_flag(Flag.REJECTED):
            raise ValueError("Not a rejected state as expected by reject directive: " + str(state))
        self.state = state


class Coordination(ABC):

    @property
    def parameters(self):
        """Sequence of tuples representing arbitrary immutable sync parameters"""
        return None

    @abstractmethod
    def coordinate(self, instance) -> Directive:
        """
        If returned signal is 'WAIT' then the job is obligated to call :func:`wait_and_release`
        which will likely suspend the job until an awaited condition is changed.

        :param instance:
        :param: job_info job for which the signal is being set
        :return: sync state for job
        """


class ExecutionsLimitation(Coordination, ExecutionStateEventObserver):

    def __init__(self, execution_group, max_executions, state_receiver_factory=StateReceiver):
        if not execution_group:
            raise ValueError('Execution group must be specified')
        if max_executions < 1:
            raise ValueError('Max executions must be greater than zero')
        self._group = execution_group
        self._max = max_executions
        self._state_receiver_factory = state_receiver_factory

        self._wait_guard = Condition()
        # vv Guarding these fields vv
        self._wait_counter = 0
        self._current_wait = None
        self._state_receiver = None

        self._parameters = (
            ('sync', 'executions_limitation'),
            ('execution_group', execution_group),
            ('max_executions', max_executions)
        )

    class _Wait(Wait):

        def __init__(self, outer: "ExecutionsLimitation", wait_id):
            super().__init__(ExecutionState.QUEUED, WaitCondition.EXEC_GROUP)
            self.outer = outer
            self.wait_id = wait_id

        def wait(self):
            log.debug("event=[exec_limit_coord_wait_starting]")
            self.outer._wait(self.wait_id)
            log.debug("event=[exec_limit_coord_wait_finished]")

        def release(self):
            self.outer._release()

    @property
    def execution_group(self):
        return self._group

    @property
    def max_executions(self):
        return self._max

    @property
    def parameters(self):
        return self._parameters

    def _allowed_continue(self, job_inst) -> bool:
        # TODO Set phase filters + params filters
        jobs, _ = taro.client.read_instances()

        group_jobs_sorted = JobInstances(sorted(jobs, key=lambda job: job.lifecycle.changed_at(ExecutionState.CREATED)))
        next_count = self.max_executions - len(group_jobs_sorted.executing)
        if next_count <= 0:
            return False

        for next_ready in group_jobs_sorted.scheduled[0:next_count]:  # TODO Change to queued

            job_created = job_inst.lifecycle.changed_at(ExecutionState.CREATED)
        for allowed in next_ready:
            if job_inst.id == allowed.id or job_created <= allowed.lifecycle.changed_at(ExecutionState.CREATED):
                # The second condition ensure this works even when the job is not contained in the list for any reasons
                return True

        return False

    @timing('exec_limit_set_signal', args_idx=[1])
    def coordinate(self, instance) -> Directive:
        if self._allowed_continue(instance):
            return CONTINUE

        with self._wait_guard:
            current_wait = self._current_wait
            if not current_wait:
                current_wait = self._setup_waiting()
                if self._allowed_continue(instance):
                    self._remove_waiting()
                    return CONTINUE

        return self._Wait(self, current_wait)

    def _setup_waiting(self):
        self._start_listening()

        self._wait_counter += 1
        self._current_wait = self._wait_counter
        return self._current_wait

    def _start_listening(self):
        self._state_receiver = self._state_receiver_factory()
        self._state_receiver.listeners.append(self)
        self._state_receiver.start()

    def state_update(self, instance_meta, _, new_state, __):
        with self._wait_guard:
            if not self._current_wait:
                return
            if new_state.in_phase(ExecutionPhase.TERMINAL) and self._is_same_exec_group(instance_meta):
                self._remove_waiting()
                self._wait_guard.notify_all()

    def _remove_waiting(self):
        self._current_wait = None
        self._stop_listening()

    def _stop_listening(self):
        self._state_receiver.close()
        self._state_receiver.listeners.remove(self)
        self._state_receiver = None

    def _wait(self, wait_id):
        log.debug("event=[exec_limit_coord_wait_starting]")

        with self._wait_guard:
            if not self._current_wait or self._current_wait != wait_id:
                return
            self._wait_guard.wait()

        log.debug("event=[exec_limit_coord_wait_finished]")

    def _release(self):
        with self._wait_guard:
            self._remove_waiting()
            self._wait_guard.notify_all()


# TODO delete
def create_composite(executions_limit: ExecutionGroupLimit = None, no_overlap: bool = False,
                     depends_on: Sequence[str] = ()):
    syncs = []

    if executions_limit:
        limitation = ExecutionsLimitation(executions_limit.group, executions_limit.max_executions)
        syncs.append(limitation)
    if no_overlap:
        syncs.append(NoOverlap())
    if depends_on:
        syncs.append(Dependency(*depends_on))

    return syncs
