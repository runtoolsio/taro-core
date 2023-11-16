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
from abc import ABC, abstractmethod
from dataclasses import dataclass
from threading import Thread
from typing import Dict, Any, Optional, List

from tarotools.taro.jobs.track import TrackedTaskInfo
from tarotools.taro.run import TerminationStatus, RunState, PhaseRun, Run
from tarotools.taro.util.observer import DEFAULT_OBSERVER_PRIORITY


@dataclass
class JobInstanceMetadata:
    """
    A dataclass that contains metadata information related to a specific job run. This object is designed 
    to represent essential information about a job run in a compact and serializable format. By using this object 
    instead of a full `JobRun` snapshot, you can reduce the amount of data transmitted when sending information 
    across a network or between different parts of a system.

    Attributes:
        job_id (str):
            The unique identifier of the job associated with the instance.
        run_id (str):
            The unique identifier of the job instance run.
        instance_id (str):
            The reference identifier of the job instance.
        system_parameters (Dict[str, Any]):
            A dictionary containing system parameters for the job instance.
            These parameters are implementation-specific and contain information needed by the system to
            perform certain tasks or enable specific features.
        user_params (Dict[str, Any]):
            A dictionary containing user-defined parameters associated with the instance.
            These are arbitrary parameters set by the user, and they do not affect the functionality.
    """
    job_id: str
    run_id: str
    instance_id: str
    system_parameters: Dict[str, Any]
    user_params: Dict[str, Any]

    @classmethod
    def deserialize(cls, as_dict):
        return cls(
            as_dict['job_id'],
            as_dict['run_id'],
            as_dict['instance_id'],
            as_dict['system_parameters'],
            as_dict['user_params'],
        )

    def serialize(self) -> Dict[str, Any]:
        return {
            "job_run_id": self.job_run_id.serialize(),
            "instance_id": self.instance_id,
            "system_parameters": self.system_parameters,
            "user_params": self.user_params,
        }

    def contains_system_parameters(self, *params):
        return all(param in self.system_parameters for param in params)


class JobInstance(abc.ABC):
    """
    The `JobInstance` class is a central component of this package. It denotes a single occurrence of a job.
    While the job itself describes static attributes common to all its instances, the JobInstance class
    represents a specific run of that job.
    """

    @property
    @abc.abstractmethod
    def instance_id(self):
        """
        Returns:
            str: Instance reference/identity identifier.
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
        return self.metadata.job_id

    @property
    def run_id(self):
        """
        Returns:
            str: Run part of the instance identifier.
        """
        return self.metadata.job_id

    @property
    @abc.abstractmethod
    def tracking(self):
        """TODO: Task tracking information, None if tracking is not supported"""

    @abc.abstractmethod
    def job_run_info(self):
        """
        Creates a consistent, thread-safe snapshot of the job instance's current state.

        Returns:
            JobRun: A snapshot representing the current state of the job instance.
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
    def add_observer_phase_transition(self, observer, priority=DEFAULT_OBSERVER_PRIORITY, notify_on_register=False):
        """
        Register an instance state observer. Optionally, trigger a notification with the last known state
        upon registration.

        Notes for implementers: Prevent race-conditions when `notify_on_register` used.

        Args:
            observer:
                The observer to register. This can either be:
                1. An instance of `InstanceStateObserver`.
                2. A callable object with the signature of the `InstanceStateObserver.instance_phase_transition` method.
            priority (int, optional):
                Priority of the observer. Lower numbers are notified first.
            notify_on_register (bool, optional):
                If True, immediately notifies the observer about the last known instance state change upon registration.
        """

    @abc.abstractmethod
    def remove_observer_phase_transition(self, observer):
        """
        De-register an execution state observer.
        Note: The implementation must cope with the scenario when this method is executed during notification.

        Args:
            observer: The observer to de-register.
        """

    @abc.abstractmethod
    def add_observer_status(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
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
    def remove_observer_status(self, observer):
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


@dataclass(frozen=True)
class InstanceMetadata:
    instance_id: str


@dataclass(frozen=True)
class JobRun:
    """
    Immutable snapshot of job instance
    TODO: Instance detail/info
    """

    """Descriptive information about this job run"""
    metadata: JobInstanceMetadata
    """The snapshot of the job run represented by this instance"""
    run: Run
    # TODO textwrap.shorten(status, 1000, placeholder=".. (truncated)", break_long_words=False)
    tracking: TrackedTaskInfo

    @classmethod
    def deserialize(cls, as_dict):
        return cls(
            JobInstanceMetadata.deserialize(as_dict['metadata']),
            Run.deserialize(as_dict['run']),
            TrackedTaskInfo.from_dict(as_dict['tracking']) if "tracking" in as_dict else None,
        )

    def serialize(self, include_empty=True) -> Dict[str, Any]:
        return {
            "metadata": self.metadata.serialize(),
            "run": self.run.serialize(),
            "tracking": self.tracking.to_dict(include_empty) if self.tracking else None,
        }

    @property
    def job_id(self) -> str:
        """
        Returns:
            str: Job part of the instance identifier.
        """
        return self.metadata.job_id

    @property
    def run_id(self) -> str:
        """
        Returns:
            str: Run part of the instance identifier.
        """
        return self.metadata.run_id


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


class PhaseTransitionObserver(abc.ABC):

    @abc.abstractmethod
    def new_phase(self, job_run: JobRun, previous_phase: PhaseRun, new_phase: PhaseRun, ordinal: int):
        """
        Called when the instance transitions to a new phase.

        The notification can optionally happen also when this observer is registered with the instance
        to make the observer aware about the current phase of the instance.

        Args:
            job_run (JobInstSnapshot): A snapshot of the job instance that transitioned to a new phase.
            previous_phase (TerminationStatus): The previous phase of the job instance.
            new_phase (TerminationStatus): The new/current phase state of the job instance.
            ordinal (int): The number of the current phase.
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


class InstanceOutputObserver(abc.ABC):

    @abc.abstractmethod
    def new_instance_output(self, job_info: JobRun, output: str, is_error: bool):
        """
        Executed when a new output line is available.

        Args:
            job_info (JobInstSnapshot): Job instance producing the output.
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
