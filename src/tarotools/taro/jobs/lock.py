"""
This module provides the logic required for the locking mechanisms used by specific parts of the library.

Locks:
1. State Lock
Several features provide synchronization among job instances. To ensure their proper functioning, it is essential
to control when an active job instance can change its state. To achieve this, the instance must obtain the state lock.
While an instance holds the lock, only that instance is permitted to change its state.
Therefore, any part of the code possessing the lock can depend on the current immutable state of all instances (using
the same lock).
"""

import contextlib
import logging
import random
import time
from abc import ABC, abstractmethod

import portalocker

from tarotools.taro import cfg
from tarotools.taro import paths

log = logging.getLogger(__name__)


class StateLocker(ABC):
    """
    Manages locking for instances that either:
    1. Intend to change their state (without considering the states of other instances).
    2. Need to determine if they can modify their state based on the states of other instances.

    While a lock is held by one instance, no other instances can change their state.
    """

    @abstractmethod
    def __call__(self):
        """
        Attempts to acquire the state lock and returns it to the caller.

        Returns:
             StateLock: The acquired state lock in its locked state.
        """


class StateLock(ABC):
    """
    Represents a state lock returned by a state locker. This lock can be passed to another component, which
    is then responsible for unlocking it when a specified condition is met.
    """

    @abstractmethod
    def unlock(self):
        """
        Unlock the state lock
        """


class PortalockerStateLocker(StateLocker):
    """
    A state locker implementation that uses a shared file lock provided by the Portalocker library.
    The lock file should be accessible only by the current user to ensure security.

    Attributes:
        lock_file (str, Path):
            Path to the file used for locking.
        timeout (float, optional):
            Maximum duration (in seconds) to wait for the lock before giving up.
            Defaults to the value of `cfg.lock_timeout_sec`.
        max_check_time (float, optional):
            Maximum duration (in seconds) between lock acquisition attempts.
            Defaults to the value of `cfg.lock_max_check_time_sec`.
    """

    def __init__(self, lock_file, *, timeout=None, max_check_time=None):
        self.lock_file = lock_file
        self.timeout = timeout or cfg.lock_timeout_sec
        self.max_check_time = max_check_time or cfg.lock_max_check_time_sec

    def _check_interval(self):
        """
        Determines the interval between lock acquisition attempts. Using a constant interval could lead
        to lock starvation when multiple instances try to acquire the lock at the same time.

        Returns:
             int: A random interval (in seconds) between 10 milliseconds and the max check time.
        """
        return random.randint(10, self.max_check_time * 1000) / 1000

    @contextlib.contextmanager
    def __call__(self):
        """
        Attempts to acquire the state lock using Portalocker.
        If successful, yields a PortalockerStateLock for potential use by the caller.
        When the context is exited (e.g., at the end of a 'with' block), the lock is automatically unlocked.
        If used outside a context manager scope, the caller should manually ensure the lock is unlocked.

        Yields:
            PortalockerStateLock: The acquired state lock, if successful.
        """
        file_lock = portalocker.Lock(self.lock_file, timeout=self.timeout, check_interval=self._check_interval())
        start_time = time.time()
        file_lock.acquire()
        log.debug(f'event=[coord_lock_acquired] wait=[{(time.time() - start_time) * 1000 :.2f} ms]')

        lock = PortalockerStateLock(file_lock)
        try:
            yield lock
        finally:
            lock.unlock()


class PortalockerStateLock(StateLock):
    """
    Represents a state lock using Portalocker, keeping track of its creation and unlock times.

    Attributes:
        file_lock: The lock file object.
        created_at: Timestamp when the lock was created.
        unlocked_at: Timestamp when the lock was unlocked, or None if still locked.
    """

    def __init__(self, file_lock):
        """
        Initialize the PortalockerStateLock with the given locked file.
        """
        self.file_lock = file_lock
        self.created_at = time.time()
        self.unlocked_at = None

    def unlock(self):
        """
        Unlock the associated locked file and log the duration for which it was locked.
        """
        if self.unlocked_at:
            return

        self.file_lock.release()
        self.unlocked_at = time.time()
        lock_time_ms = (self.unlocked_at - self.created_at) * 1000
        log.debug(f'event=[coord_lock_released] locked=[{lock_time_ms:.2f} ms]')


class NullStateLocker(StateLocker, StateLock):
    """
    A no-op implementation of StateLocker and StateLock. It is useful for testing where locking is not required.
    """

    @contextlib.contextmanager
    def __call__(self, *args, **kwargs):
        """
        Yield itself without performing any locking operations.
        """
        yield self

    def unlock(self):
        """
        No-op unlock method.
        """
        pass


def default_coord_locker():
    return PortalockerStateLocker(paths.lock_path('state0.lock', True))
