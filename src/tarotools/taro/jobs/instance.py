"""
This module defines the instance part of the job framework and is built on top of the execution framework defined
in the execution module.

The main parts are:
1. The job instance abstraction: An interface of job instance
2. `JobInst` class: An immutable snapshot of a job instance
3. Job instance observers

Note: See the `runner` module for the default job instance implementation
TODO:
1. Add `labels`
"""

import abc
import datetime
import textwrap
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum, auto
from fnmatch import fnmatch
from functools import partial
from threading import Thread
from typing import NamedTuple, Dict, Any, Optional, List, Tuple, Iterable

from tarotools.taro import util
from tarotools.taro.jobs.criteria import IDMatchCriteria
from tarotools.taro.jobs.track import TrackedTaskInfo
from tarotools.taro.run import TerminationStatus, FailedRun
from tarotools.taro.util import is_empty, format_dt_iso
from tarotools.taro.util.observer import DEFAULT_OBSERVER_PRIORITY


class InstancePhase(Enum):
    NONE = auto()
    CREATED = auto()
    PENDING = auto()
    QUEUED = auto()
    EXECUTING = auto()
    TERMINAL = auto()

    def is_before(self, other_phase):
        return self.value < other_phase.value

    def is_after(self, other_phase):
        return self.value > other_phase.value

    def __call__(self, lifecycle):
        return self.transitioned_at(lifecycle)

    def transitioned_at(self, lifecycle) -> Optional[datetime.datetime]:
        return lifecycle.transitioned_at(self)


class LifecycleEvent(Enum):
    # TODO Remove and use phase instead
    CREATED = partial(lambda l: l.created_at)
    EXECUTED = partial(lambda l: l.executed_at)
    ENDED = partial(lambda l: l.ended_at)

    def __call__(self, instance) -> Optional[datetime.datetime]:
        return self.value(instance)

    @classmethod
    def decode(cls, value):
        return cls[value]

    def encode(self):
        return self.name


class InstanceLifecycle:
    """
    This class represents the lifecycle of an instance. A lifecycle consists of a chronological sequence of
    instance phases. Each phase has a timestamp that indicates when the instance transitioned to that phase.
    """

    def __init__(self, *phase_changes: Tuple[InstancePhase, datetime.datetime],
                 termination_status: TerminationStatus = TerminationStatus.NONE):
        self._phase_changes: OrderedDict[InstancePhase, datetime.datetime] = OrderedDict(phase_changes)
        self._terminal_status = termination_status

    @classmethod
    def from_dict(cls, as_dict):
        phase_changes = ((InstancePhase[state_change['phase']], util.parse_datetime(state_change['changed']))
                         for state_change in as_dict['phase_changes'])
        return cls(*phase_changes)

    @property
    def phase(self):
        return next(reversed(self._phase_changes.keys()), InstancePhase.NONE)

    @property
    def phases(self) -> List[InstancePhase]:
        return list(self._phase_changes.keys())

    @property
    def phase_changes(self) -> Iterable[Tuple[InstancePhase, datetime.datetime]]:
        return ((phase, changed) for phase, changed in self._phase_changes.items())

    def changed_at(self, phase: InstancePhase) -> datetime.datetime:
        return self._phase_changes[phase]

    @property
    def last_changed_at(self) -> Optional[datetime.datetime]:
        return next(reversed(self._phase_changes.values()), None)

    @property
    def created_at(self) -> datetime.datetime:
        return self.changed_at(InstancePhase.CREATED)

    @property
    def is_executed(self) -> bool:
        return InstancePhase.EXECUTING in self._phase_changes

    @property
    def executed_at(self) -> Optional[datetime.datetime]:
        return self._phase_changes.get(InstancePhase.EXECUTING)

    @property
    def is_ended(self):
        return InstancePhase.TERMINAL in self._phase_changes

    @property
    def ended_at(self) -> Optional[datetime.datetime]:
        return self.changed_at(InstancePhase.TERMINAL)

    @property
    def execution_time(self) -> Optional[datetime.timedelta]:
        start = self.executed_at
        if not start:
            return None

        end = self.ended_at or util.utc_now()
        return end - start

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        d = {
            "phase_changes": [{"state": state.name, "changed": format_dt_iso(change)} for state, change in
                              self.phase_changes],
            "phase": self.phase.name,
            "last_changed_at": format_dt_iso(self.last_changed_at),
            "created_at": format_dt_iso(self.created_at),
            "executed_at": format_dt_iso(self.executed_at),
            "ended_at": format_dt_iso(self.ended_at),
            "execution_time": self.execution_time.total_seconds() if self.executed_at else None,
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    def __copy__(self):
        copied = InstanceLifecycle()
        copied._phase_changes = self._phase_changes
        return copied

    def __deepcopy__(self, memo):
        return InstanceLifecycle(*self.phase_changes)

    def __eq__(self, other):
        if not isinstance(other, InstanceLifecycle):
            return NotImplemented
        return self._phase_changes == other._phase_changes

    def __hash__(self):
        return hash(tuple(self._phase_changes.items()))

    def __repr__(self) -> str:
        return "{}({!r})".format(
            self.__class__.__name__, self._phase_changes)


class JobInstanceID(NamedTuple):
    """
    Attributes:
        job_id (str): The ID of the job to which the instance belongs.
        instance_id (str): The ID of the individual instance.

    TODO:
        1. Identity or reference ID.
        2. Create a method that returns a match and a no-match for this ID.
    """
    job_id: str
    instance_id: str

    @classmethod
    def from_dict(cls, as_dict):
        return cls(as_dict['job_id'], as_dict['instance_id'])

    def matches_pattern(self, id_pattern, matching_strategy=fnmatch):
        return IDMatchCriteria.parse_pattern(id_pattern, strategy=matching_strategy).matches(self)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "instance_id": self.instance_id
        }

    def __eq__(self, other):
        if type(self) is type(other):
            return self.job_id == other.job_id and self.instance_id == other.instance_id
        else:
            return False

    def __hash__(self):
        return hash((self.job_id, self.instance_id))

    def __repr__(self):
        return "{}@{}".format(self.job_id, self.instance_id)


@dataclass
class JobInstanceMetadata:
    """
    A dataclass that contains metadata information related to a specific job instance.
    This object is designed to represent essential information about a job instance in a compact and
    serializable format. By using this object instead of a full job instance snapshot, you can reduce the amount of
    data transmitted when sending information across a network or between different parts of a system.

    Attributes:
        id (JobInstanceID):
            The unique identifier associated with the job instance.
        parameters (Tuple[Tuple[str, str]]):
            A tuple of key-value pairs representing system parameters for the job.
            These parameters are implementation-specific and contain information needed by the system to
            perform certain tasks or enable specific features.
        user_params (Dict[str, Any]):
            A dictionary containing user-defined parameters associated with the instance.
            These are arbitrary parameters set by the user, and they do not affect the functionality.
    """
    id: JobInstanceID
    parameters: Tuple[Tuple[str, str]]  # TODO Rename to system_params
    user_params: Dict[str, Any]

    @classmethod
    def from_dict(cls, as_dict):
        return cls(JobInstanceID.from_dict(as_dict['id']), as_dict['parameters'], as_dict['user_params'])

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        d = {
            "id": self.id.to_dict(),
            "parameters": self.parameters,
            "user_params": self.user_params,
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    def contains_parameters(self, *params):
        return all(param in self.parameters for param in params)

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"job_id={self.id!r}, "
            f"parameters={self.parameters!r}, "
            f"user_params={self.user_params!r})"
        )


class JobInstance(abc.ABC):
    """
    The `JobInstance` class is a central component of this package. It denotes a single occurrence of a job.
    While the job itself describes static attributes common to all its instances, the JobInstance class
    represents a specific execution or run of that job.
    """

    @property
    @abc.abstractmethod
    def metadata(self):
        """
        Returns:
            JobInstanceMetadata: Descriptive information about this instance.
        """

    @property
    def id(self):
        """
        Returns:
            JobInstanceID: Identifier of this instance.
        """
        return self.metadata.id

    @property
    def job_id(self):
        """
        Returns:
            str: Job part of the instance identifier.
        """
        return self.id.job_id

    @property
    def instance_id(self):
        """
        Returns:
            str: Instance part of the instance identifier.
        """
        return self.id.instance_id

    @property
    @abc.abstractmethod
    def lifecycle(self):
        """
        Retrieves the execution lifecycle of this instance.

        The execution lifecycle comprises a sequence of states that the job instance transitions through,
        each associated with a timestamp indicating when that state was reached.

        Returns:
            InstanceLifecycle: The lifecycle of this job instance.
        """

    @property
    @abc.abstractmethod
    def tracking(self):
        """TODO: Task tracking information, None if tracking is not supported"""

    @property
    @abc.abstractmethod
    def status(self):
        """
        Returns:
            str: Current status of the job or None if not supported
        """

    @property
    @abc.abstractmethod
    def last_output(self):
        """
        Retrieves the recent lines of job output.

        Each line is returned as a tuple, where the first element is the output string and the second element
        is a boolean value indicating whether the output is an error output.

        The number of lines returned is dependent on the specific implementation. It is not recommended to return
        large number of lines.

        Returns:
            List[Tuple[str, bool]]: A list of the latest output lines or None if output capture is not supported.
        """

    @property
    @abc.abstractmethod
    def error_output(self):
        """
        Retrieves the lines of error output.

        Returns:
            List[str] or None: Lines of error output, or None if error output capture is not supported.
        """

    @property
    @abc.abstractmethod
    def warnings(self):
        """
        Retrieves the warnings associated with the job instance.

        Returns:
            Dict[str, int]: A dictionary mapping warning names to their occurrence count.
        """

    @property
    @abc.abstractmethod
    def exec_error(self):
        """
        Retrieves the error details of the job execution, if any occurred.
        If no errors occurred during the execution of the job, this property returns None.

        Returns:
            tarotools.taro.jobs.lifecycle.ExecutionError: The details of the execution error or None if the job executed successfully.
        """

    @property
    @abc.abstractmethod
    def queue_waiter(self):
        """
        Returns:
            Optional[QueueWaiter]: Queue waiter if the instance has been assigned to an execution queue
        """

    @abc.abstractmethod
    def create_snapshot(self):
        """
        Creates a consistent, thread-safe snapshot of the job instance's current state.

        Returns:
            JobInst: A snapshot representing the current state of the job instance.
        """

    @abc.abstractmethod
    def release(self):
        """
        Releases the instance if it's in the pre-execution phase, waiting for a specific condition to be met
        before it begins execution.

        This method is intended for two primary scenarios:
        1. When the job is awaiting an external condition and relies on the user to signal when that condition is met.
        2. When the user, understanding the implications, chooses to bypass the waiting condition and releases
           the instance prematurely.
        """

    @abc.abstractmethod
    def stop(self):
        """
        Attempts to cancel a scheduled job or stop a job that is already executing.

        Note:
            The way the stop request is handled can vary based on the implementation or the specific job.
            It's possible that not all instances will respond successfully to the stop request.
        """

    @abc.abstractmethod
    def interrupted(self):
        """
        TODO: Notify about keyboard interruption signal
        """

    @abc.abstractmethod
    def add_warning(self, warning):
        """
        Adds a warning to the job instance.

        Args:
            warning (Warn): The warning to be added.
        """

    @abc.abstractmethod
    def add_state_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY, notify_on_register=False):
        """
        Register an instance state observer. Optionally, trigger a notification with the last known state
        upon registration.

        Notes for implementers: Prevent race-conditions when `notify_on_register` used.

        Args:
            observer:
                The observer to register. This can either be:
                1. An instance of `InstanceStateObserver`.
                2. A callable object with the signature of the `InstanceStateObserver.new_instance_state` method.
            priority (int, optional):
                Priority of the observer. Lower numbers are notified first.
            notify_on_register (bool, optional):
                If True, immediately notifies the observer about the last known instance state change upon registration.
        """

    @abc.abstractmethod
    def remove_state_observer(self, observer):
        """
        De-register an execution state observer.
        Note: The implementation must cope with the scenario when this method is executed during notification.

        Args:
            observer: The observer to de-register.
        """

    @abc.abstractmethod
    def add_warning_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        """
        Register a warning observer.
        The observer can be:
            1. An instance of `WarningObserver`.
            2. A callable object with the signature of `WarningObserver.new_warning`.

        Args:
            observer:
                The observer to register.
            priority (int, optional):
                The observer's priority as a number. Lower numbers are notified first.
        """

    @abc.abstractmethod
    def remove_warning_observer(self, observer):
        """
        De-register a warning observer.

        Args:
            observer: The observer to de-register.
        """

    @abc.abstractmethod
    def add_output_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        """
        Register an output observer.
        The observer can be:
            1. An instance of `InstanceOutputObserver`.
            2. A callable object with the signature of `InstanceOutputObserver.instance_output_update`.

        Args:
            observer: The observer to register.
            priority (int, optional): The observer's priority as a number. Lower numbers are notified first.
        """

    @abc.abstractmethod
    def remove_output_observer(self, observer):
        """
        De-register an output observer.

        Args:
            observer: The observer to de-register.
        """


class RunnableJobInstance(JobInstance):

    @abc.abstractmethod
    def run(self):
        """
        Run the job.

        This method is not expected to raise any errors. In case of any failure the error details can be retrieved
        by calling `exec_error` method.
        """


class DelegatingJobInstance(RunnableJobInstance):

    def __init__(self, delegated):
        self.delegated = delegated

    @abc.abstractmethod
    def run(self):
        """Run the job"""

    @property
    def id(self):
        return self.delegated.id

    @property
    def metadata(self):
        return self.delegated.metadata

    @property
    def lifecycle(self):
        return self.delegated.lifecycle

    @property
    def status(self):
        return self.delegated.status

    @property
    def tracking(self):
        return self.delegated.tracking

    @property
    def last_output(self):
        return self.delegated.last_output

    @property
    def error_output(self):
        return self.delegated.error_output

    @property
    def warnings(self):
        return self.delegated.warnings

    def add_warning(self, warning):
        self.delegated.add_warning(warning)

    @property
    def exec_error(self) -> FailedRun:
        return self.delegated.exec_error

    def create_snapshot(self):
        return self.delegated.create_snapshot()

    def release(self):
        self.delegated.release()

    def stop(self):
        self.delegated.stop()

    def interrupted(self):
        self.delegated.interrupted()

    def add_state_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY, notify_on_register=False):
        self.delegated.add_state_observer(observer, priority, notify_on_register)

    def remove_state_observer(self, observer):
        self.delegated.remove_state_observer(observer)

    def add_warning_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self.delegated.add_warning_observer(observer, priority)

    def remove_warning_observer(self, observer):
        self.delegated.remove_warning_observer(observer)

    def add_output_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self.delegated.add_output_observer(observer, priority)

    def remove_output_observer(self, observer):
        self.delegated.remove_output_observer(observer)


class RunInNewThreadJobInstance(DelegatingJobInstance):

    def __init__(self, job_instance):
        super().__init__(job_instance)

    def run(self):
        t = Thread(target=self.delegated.run)
        t.start()


class JobInst:
    """
    Immutable snapshot of job instance
    """

    @classmethod
    def from_dict(cls, as_dict):
        if as_dict['tracking']:
            tracking = TrackedTaskInfo.from_dict(as_dict['tracking'])
        else:
            tracking = None

        if as_dict['exec_error']:
            exec_error = FailedRun.from_dict(as_dict['exec_error'])
        else:
            exec_error = None

        return cls(
            JobInstanceMetadata.from_dict(as_dict['metadata']),
            InstanceLifecycle.from_dict(as_dict['lifecycle']),
            tracking,
            as_dict['status'],
            as_dict['error_output'],
            as_dict['warnings'],
            exec_error,
        )

    def __init__(self, metadata, lifecycle, tracking, status, error_output, warnings, exec_error):
        self._metadata = metadata
        self._lifecycle = lifecycle
        self._tracking = tracking
        if status:
            self._status = textwrap.shorten(status, 1000, placeholder=".. (truncated)", break_long_words=False)
        else:
            self._status = status
        self._error_output = error_output or ()
        self._warnings = warnings or {}
        self._exec_error = exec_error

    @staticmethod
    def created_at(job_info):
        return job_info.lifecycle.created_at

    @property
    def job_id(self) -> str:
        """
        Returns:
            str: Job part of the instance identifier.
        """
        return self.metadata.id.job_id

    @property
    def instance_id(self) -> str:
        """
        Returns:
            str: Instance part of the instance identifier.
        """
        return self.metadata.id.instance_id

    @property
    def id(self):
        """
        Returns:
            JobInstanceID: Identifier of this instance.
        """
        return self.metadata.id

    @property
    def metadata(self):
        """
        Returns:
            JobInstanceMetadata: Descriptive information about this instance.
        """
        return self._metadata

    @property
    def lifecycle(self):
        """
        Retrieves the execution lifecycle of this instance.

        The execution lifecycle comprises a sequence of states that the job instance transitions through,
        each associated with a timestamp indicating when that state was reached.

        Returns:
            InstanceLifecycle: The lifecycle of this job instance.
        """
        return self._lifecycle

    @property
    def state(self):
        """
        Returns:
            The current execution state of the instance
        """
        return self._lifecycle.phase

    @property
    def tracking(self):
        """TODO: Task tracking information, None if tracking is not supported"""
        return self._tracking

    @property
    def status(self):
        """
        Returns:
            str: Current status of the job or None if not supported.
        """
        return self._status

    @property
    def warnings(self):
        """
        Retrieves the warnings associated with the job instance.
        TODO:
            1. Rename to `warning_counts` or so (maybe just move to `JobInst`)
            2. Make `warnings` returned the `Warn` objects (impl. limit)
            3. Modify de/serialization accordingly
            4. Associated timestamps or add timestamp to `Warn` object?

        Returns:
            Dict[str, int]: A dictionary mapping warning names to their occurrence count.
        """
        return self._warnings

    @property
    def error_output(self):
        """
        Retrieves the lines of error output.

        Returns:
            List[str] or None: Lines of error output, or None if error output capture is not supported.
        """
        return self._error_output

    @property
    def exec_error(self):
        """
        Retrieves the error details of the job execution, if any occurred.
        If no errors occurred during the execution of the job, this property returns None.

        Returns:
            tarotools.taro.jobs.lifecycle.ExecutionError: The details of the execution error or None if the job executed successfully.
        """
        return self._exec_error

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        d = {
            "metadata": self.metadata.to_dict(include_empty),
            "lifecycle": self.lifecycle.to_dict(include_empty),
            "tracking": self.tracking.to_dict(include_empty) if self.tracking else None,
            "status": self.status,
            "error_output": self.error_output,
            "warnings": self.warnings,
            "exec_error": self.exec_error.to_dict(include_empty) if self.exec_error else None
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    def __eq__(self, other):
        if not isinstance(other, JobInst):
            return NotImplemented
        return (self.metadata, self._lifecycle, self._tracking, self._status, self._error_output,
                self._warnings, self._exec_error) == \
            (other.metadata, other._lifecycle, other._tracking, other._status, other._error_output,
             other._warnings, other._exec_error)  # TODO

    def __hash__(self):
        return hash((self.metadata, self._lifecycle, self._tracking, self._status, self._error_output,
                     tuple(sorted(self._warnings.items())), self._exec_error))  # TODO

    def __repr__(self):
        return f"{self.__class__.__name__}("f"metadata={self.metadata!r}"


class JobInstances(list):
    """
    List of job instances with auxiliary methods.
    """

    def __init__(self, jobs):
        super().__init__(jobs)

    @property
    def job_ids(self) -> List[str]:
        return [j.id.job_id for j in self]

    def in_phase(self, execution_phase):
        return [j for j in self if j.lifecycle.phase.phase is execution_phase]

    def in_state(self, execution_state):
        return [j for j in self if j.lifecycle.phase is execution_state]

    @property
    def scheduled(self):
        return self.in_phase(InstancePhase.CREATED)

    @property
    def pending(self):
        return self.in_phase(InstancePhase.PENDING)

    @property
    def queued(self):
        return self.in_phase(InstancePhase.QUEUED)

    @property
    def executing(self):
        return self.in_phase(InstancePhase.EXECUTING)

    @property
    def terminal(self):
        return self.in_phase(InstancePhase.TERMINAL)

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        return {"jobs": [job.to_dict(include_empty=include_empty) for job in self]}


class InstancePhaseObserver(abc.ABC):

    @abc.abstractmethod
    def new_instance_phase(self, job_inst: JobInst, previous_phase: TerminationStatus, new_phase: TerminationStatus,
                           changed: datetime.datetime):
        """
        Called when the instance transitions to a new phase.

        The notification can optionally happen also when this observer is registered with the instance
        to make the observer aware about the current phase of the instance.

        Args:
            job_inst (JobInst): The job instance that transitioned to a new phase.
            previous_phase (TerminationStatus): The previous phase of the job instance.
            new_phase (TerminationStatus): The new/current phase state of the job instance.
            changed (datetime.datetime): The timestamp of when the transition.
        """


@dataclass
class Warn:
    """
    This class represents a warning.

    Attributes:
        name (str): Name is used to identify the type of the warning.
        params (Optional[Dict[str, Any]]): Arbitrary parameters related to the warning.
    """
    name: str
    params: Optional[Dict[str, Any]] = None


@dataclass
class WarnEventCtx:
    """
    A class representing information related to a warning event.

    Attributes:
        warning (Warn): The warning which initiated the event.
        count (int): The total number of warnings with the same name associated with the instance.
    """
    warning: Warn
    count: int


class InstanceWarningObserver(abc.ABC):

    @abc.abstractmethod
    def new_instance_warning(self, job_info: JobInst, warning_ctx: WarnEventCtx):
        """This method is called when there is a new warning event."""


class InstanceOutputObserver(abc.ABC):

    @abc.abstractmethod
    def new_instance_output(self, job_info: JobInst, output: str, is_error: bool):
        """
        Executed when a new output line is available.

        Args:
            job_info (JobInst): Job instance producing the output.
            output (str): Job instance output text.
            is_error (bool): True if it is an error output, otherwise False.
        """


class JobInstanceManager(ABC):
    """
    Interface for managing job instances. The ambiguous name 'Manager' is used because the
    subclasses may implement diverse functionalities for the instances registered to this object.
    """

    @abstractmethod
    def register_instance(self, job_instance):
        """
        Register a new job instance with the manager.

        The specifics of what occurs upon registering an instance depend on the implementing class.
        The class is not required to keep track of the instance if that is not needed for the provided functionality.

        Args:
            job_instance: The job instance to be registered.
        """
        pass

    @abstractmethod
    def unregister_instance(self, job_instance):
        """
        Unregister an existing job instance from the manager.

        This will trigger any necessary clean-up or de-initialization tasks if needed. The specifics of what occurs
        upon unregistering an instance depend on the implementing class. It can be ignored if the manager does not
        track the registered instances.

        Args:
            job_instance: The job instance to be unregistered.
        """
        pass


def _job_inst_to_args(job_inst):
    states = job_inst.lifecycle.phases
    previous_state = states[-2] if len(states) > 1 else TerminationStatus.NONE
    new_state = job_inst.phase
    changed = job_inst.lifecycle.last_transition_at

    return job_inst, previous_state, new_state, changed
