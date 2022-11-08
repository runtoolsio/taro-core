import contextlib
from abc import ABC, abstractmethod

import portalocker


class StateLocker(ABC):

    @abstractmethod
    def __call__(self, *args, **kwargs):
        """
        :param args: none
        :param kwargs: none
        :return: state lock
        """


class StateLock(ABC):

    @abstractmethod
    def unlock(self):
        """
        Unlock the state lock
        """


class PortalockerStateLocker(StateLocker):

    def __init__(self, lock_file, timeout = None):
        self.lock_file = lock_file
        self.timeout = timeout

    @contextlib.contextmanager
    def __call__(self, *args, **kwargs):
        with portalocker.Lock(self.lock_file, timeout=self.timeout) as lf:
            yield PortalockerStateLock(lf)


class PortalockerStateLock(StateLock):

    def __init__(self, locked_file):
        self.locked_file = locked_file

    def unlock(self):
        portalocker.unlock(self.locked_file)
