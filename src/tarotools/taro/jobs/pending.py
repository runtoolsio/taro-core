from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from threading import Condition


@dataclass
class PendingIdentifier:
    """
    Represents a unique identifier for a pending object.

    Attributes:
        type_ (str): Specifies the type of the pending object.
        id (str): A value for distinguishing between pending objects of the same type.
    """
    type_: str
    id: str


class Pending(ABC):
    """
    Abstract base class representing a generic pending condition.

    A "pending" allows job instances to enter a pending phase before they actually start executing.
    Derived classes should provide specific implementations of the pending behavior.

    Pending either encapsulates a WAITING condition and automatically signals all associated waiters
    when this condition is SATISFIED, or it expects manual action for RELEASE when an external condition is met.
    This manual release can target all waiters simultaneously or handle each waiter individually.
    """

    @property
    @abstractmethod
    def identifier(self):
        """
        Returns the unique identifier associated with the pending object.

        Returns:
            PendingIdentifier: The identifier of the pending object.
        """
        pass

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


class WaiterState(Enum):
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
            WaiterState: The current state of the waiter.
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
        self._id = latch_id
        self._condition = Condition()
        self._released = False

    @property
    def identifier(self):
        return PendingIdentifier(type_="latch", id=self._id)

    def create_waiter(self, instance) -> "Latch._Waiter":
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
                return WaiterState.RELEASED
            if self.latch._released:
                return WaiterState.SATISFIED

            return WaiterState.WAITING

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
