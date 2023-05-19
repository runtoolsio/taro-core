import contextlib
import logging
import time
from abc import ABC, abstractmethod

import portalocker

from taro import paths

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

    def __init__(self, lock_file, timeout=None):
        self.lock_file = lock_file
        self.timeout = timeout

    @contextlib.contextmanager
    def __call__(self):
        start_time = time.time()
        with portalocker.Lock(self.lock_file, timeout=self.timeout) as lf:
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
