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
from abc import ABC, abstractmethod
from dataclasses import dataclass
from fnmatch import fnmatch
from threading import Thread
from typing import Dict, Any, Optional, List, Tuple

from tarotools.taro.jobs.criteria import IDMatchCriteria
from tarotools.taro.jobs.track import TrackedTaskInfo
from tarotools.taro.run import TerminationStatus, Phase, Lifecycle, Fault, RunState, PhaseMetadata
from tarotools.taro.util import is_empty
from tarotools.taro.util.observer import DEFAULT_OBSERVER_PRIORITY


@dataclass(frozen=True)
class JobInstanceID:
    """
    Attributes:
        job_id (str): The ID of the job to which the instance belongs.
        run_id (str): The ID of the run represented by the instance.
        instance_id (str): The reference ID of the instance.

    TODO: Create a method that returns a match and a no-match for this ID.
    """
    job_id: str
    run_id: str
    instance_id: str

    @classmethod
    def deserialize(cls, as_dict):
        return cls(as_dict['job_id'], as_dict['run_id'], as_dict['instance_id'])

    def serialize(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "run_id": self.job_id,
            "instance_id": self.instance_id
        }

    def matches_pattern(self, id_pattern, matching_strategy=fnmatch):
        return IDMatchCriteria.parse_pattern(id_pattern, strategy=matching_strategy).matches(self)


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
    def deserialize(cls, as_dict):
        return cls(JobInstanceID.deserialize(as_dict['id']), as_dict['parameters'], as_dict['user_params'])

    def serialize(self, include_empty=True) -> Dict[str, Any]:
        d = {
            "id": self.id.serialize(),
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
        # TODO needed?
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
    represents a specific run of that job.
    """

    @property
    @abc.abstractmethod
    def metadata(self):
        """
        Returns:
            JobInstanceMetadata: Descriptive information about this instance.
        """

    @property
    def job_id(self):
        """
        Returns:
            str: Job part of the instance identifier.
        """
        return self.metadata.id.job_id

    @property
    def run_id(self):
        """
        Returns:
            str: Run part of the instance identifier.
        """
        return self.metadata.id.job_id

    @property
    def instance_id(self):
        """
        Returns:
            str: Instance reference/identity identifier.
        """
        return self.metadata.id.instance_id

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
    def run_failure(self):
        """
        TODO
        """

    @property
    @abc.abstractmethod
    def run_error(self):
        """
        Retrieves the error details of the job execution, if any occurred.
        If no errors occurred during the execution of the job, this property returns None.

        Returns:
            tarotools.taro.jobs.lifecycle.ExecutionError: The details of the execution error or None if the job executed successfully.
        """

    @property
    @abc.abstractmethod
    def status_observer(self):
        """
        Returned status observer allows to add status notifications also by a logic located outside the instance.

        Returns:
            Status observer for this instance.
        """

    @abc.abstractmethod
    def create_snapshot(self):
        """
        Creates a consistent, thread-safe snapshot of the job instance's current state.

        Returns:
            JobRun: A snapshot representing the current state of the job instance.
        """
    @abc.abstractmethod
    def run(self):
        """
        Run the job.

        This method is not expected to raise any errors. In case of any failure the error details can be retrieved
        by calling `exec_error` method.
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
    def add_transition_callback(self, observer, priority=DEFAULT_OBSERVER_PRIORITY, notify_on_register=False):
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
    def remove_transition_callback(self, observer):
        """
        De-register an execution state observer.
        Note: The implementation must cope with the scenario when this method is executed during notification.

        Args:
            observer: The observer to de-register.
        """

    @abc.abstractmethod
    def add_status_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
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
    def remove_status_observer(self, observer):
        """
        De-register an output observer.

        Args:
            observer: The observer to de-register.
        """


class RunInNewThreadJobInstance:

    def __init__(self, job_instance):
        self.job_instance = job_instance

    def __getattr__(self, name):
        return getattr(self.job_instance, name)

    def run(self):
        t = Thread(target=self.delegated.run)
        t.start()


@dataclass
class JobRun:
    """
    Immutable snapshot of job instance
    """

    """Descriptive information about this instance"""
    metadata: JobInstanceMetadata
    """The lifecycle of this job run"""
    phases: Tuple[PhaseMetadata]
    lifecycle: Lifecycle
    # TODO textwrap.shorten(status, 1000, placeholder=".. (truncated)", break_long_words=False)
    tracking: TrackedTaskInfo
    termination_status: Optional[TerminationStatus]
    run_failure: Optional[Fault]
    run_error: Optional[Fault]

    @classmethod
    def deserialize(cls, as_dict):
        return cls(
            JobInstanceMetadata.deserialize(as_dict['metadata']),
            [PhaseMetadata.deserialize(phase) for phase in as_dict['phases']],
            Lifecycle.deserialize(as_dict['lifecycle']),
            TrackedTaskInfo.from_dict(as_dict['tracking']) if "tracking" in as_dict else None,
            TerminationStatus[as_dict["termination_status"]],
            Fault.deserialize(as_dict["run_failure"]) if "run_failure" in as_dict else None,
            Fault.deserialize(as_dict["run_error"]) if "run_error" in as_dict else None,
        )

    def serialize(self, include_empty=True) -> Dict[str, Any]:
        d = {
            "metadata": self.metadata.serialize(include_empty),
            "phases": [pm.serialize() for pm in self.phases],
            "lifecycle": self.lifecycle.serialize(include_empty),
            "tracking": self.tracking.to_dict(include_empty) if self.tracking else None,
            "termination_status": self.termination_status.name,
            "run_failure": self.run_failure.serialize() if self.run_failure else None,
            "run_error": self.run_error.serialize() if self.run_error else None,
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    @property
    def job_id(self) -> str:
        """
        Returns:
            str: Job part of the instance identifier.
        """
        return self.metadata.id.job_id

    @property
    def run_id(self) -> str:
        """
        Returns:
            str: Run part of the instance identifier.
        """
        return self.metadata.id.run_id

    @property
    def instance_id(self):
        """
        Returns:
            JobInstanceID: Identifier of this instance.
        """
        return self.metadata.id.instance_id


class JobRuns(list):
    """
    List of job instances with auxiliary methods.
    """

    def __init__(self, jobs):
        super().__init__(jobs)

    @property
    def job_ids(self) -> List[str]:
        return [j.id.job_id for j in self]

    def in_phase(self, phase):
        return [run for run in self if run.lifecycle.phase is phase]

    def in_state(self, state):
        return [run for run in self if run.lifecycle.run_state is state]

    @property
    def scheduled(self):
        return self.in_state(RunState.CREATED)

    @property
    def pending(self):
        return self.in_phase(RunState.PENDING)

    @property
    def queued(self):
        return self.in_phase(RunState.IN_QUEUE)

    @property
    def executing(self):
        return self.in_phase(RunState.EXECUTING)

    @property
    def terminal(self):
        return self.in_phase(RunState.ENDED)

    def to_dict(self, include_empty=True) -> Dict[str, Any]:
        return {"runs": [run.serialize(include_empty=include_empty) for run in self]}


class InstanceTransitionObserver(abc.ABC):

    @abc.abstractmethod
    def new_transition(self, job_inst: JobRun, previous_phase: Phase, new_phase: Phase, ordinal: int,
                       changed: datetime.datetime):
        """
        Called when the instance transitions to a new phase.

        The notification can optionally happen also when this observer is registered with the instance
        to make the observer aware about the current phase of the instance.

        Args:
            job_inst (JobRun): The job instance that transitioned to a new phase.
            previous_phase (TerminationStatus): The previous phase of the job instance.
            new_phase (TerminationStatus): The new/current phase state of the job instance.
            ordinal (int): The number of the current phase.
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
    def new_instance_warning(self, job_info: JobRun, warning_ctx: WarnEventCtx):
        """This method is called when there is a new warning event."""


class InstanceOutputObserver(abc.ABC):

    @abc.abstractmethod
    def new_instance_output(self, job_info: JobRun, output: str, is_error: bool):
        """
        Executed when a new output line is available.

        Args:
            job_info (JobRun): Job instance producing the output.
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
