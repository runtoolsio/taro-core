"""
This module provides the logic required for the locking mechanisms used by specific parts of the library.

Locks:
1. State Lock
Several features enable synchronization among job instances. To ensure their proper functioning, it is essential
to control when an active job instance can change its state. To do this, the instance must obtain the state lock.
While an instance holds the lock, only that instance is permitted to change its state.
Therefore, any part of the code possessing the lock can depend on the current immutable state of all instances.
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

    @abstractmethod
    def __call__(self):
        """
        :return: state lock
        """


class StateLock(ABC):

    @abstractmethod
    def unlock(self):
        """
        Unlock the state lock
        """


class PortalockerStateLocker(StateLocker):

    def __init__(self, lock_file, *, timeout=cfg.lock_timeout_ms, max_check_time_ms=cfg.lock_max_check_time_ms):
        self.lock_file = lock_file
        self.timeout = timeout
        self.max_check_time_ms = max_check_time_ms

    def _check_interval(self):
        """
        Never use a constant value for the interval, as it can lead to lock starvation in scenarios
        where multiple instances attempt to acquire the lock simultaneously.

        :return: random value between 10 and max check time param
        """
        return random.randint(10, self.max_check_time_ms) / 1000

    @contextlib.contextmanager
    def __call__(self):
        start_time = time.time()
        with portalocker.Lock(self.lock_file, timeout=self.timeout, check_interval=self._check_interval()) as lf:
            elapsed_time_ms = (time.time() - start_time) * 1000
            log.debug(f'event=[state_lock_acquired] wait=[{elapsed_time_ms:.2f} ms]')

            lock = PortalockerStateLock(lf)
            yield lock
            if not lock.unlocked_at:
                lock.unlocked_at = time.time()
                lock.log_released()


class PortalockerStateLock(StateLock):

    def __init__(self, locked_file):
        self.locked_file = locked_file
        self.created_at = time.time()
        self.unlocked_at = None

    def unlock(self):
        portalocker.unlock(self.locked_file)
        self.unlocked_at = time.time()
        self.log_released()

    def log_released(self):
        lock_time_ms = (self.unlocked_at - self.created_at) * 1000
        log.debug(f'event=[state_lock_released] locked=[{lock_time_ms:.2f} ms]')


class NullStateLocker(StateLocker, StateLock):
    @contextlib.contextmanager
    def __call__(self, *args, **kwargs):
        yield self

    def unlock(self):
        pass


def default_state_locker():
    return PortalockerStateLocker(paths.lock_path('state0.lock', True))
