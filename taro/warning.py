import abc
from collections import namedtuple
from threading import Event, Thread

from taro import JobInfo

Warn = namedtuple('Warn', 'type params')


class JobWarningObserver(abc.ABC):

    @abc.abstractmethod
    def warning_added(self, job_info: JobInfo, warning: Warn):
        """This method is called when job instance execution state is changed."""

    @abc.abstractmethod
    def warning_removed(self, job_info: JobInfo, warning: Warn):
        """This method is called when job instance execution state is changed."""


class WarningCheck(abc.ABC):

    def next_check(self, job_instance) -> int:
        """
        Returns maximum time in seconds after next check must be performed.
        However next check can be performed anytime sooner than after then interval specified by this method.

        :param job_instance: checked job
        :return: next latest check
        """

    def check(self, job_instance):
        """
        Check warning condition and return Warn object if the warning condition is met otherwise return None.

        :param job_instance: checked job
        :return: warning or None
        """


class WarningChecking:

    def __init__(self, job_control, *warning):
        self._job_control = job_control
        self._warnings = tuple(*warning)
        self._wait = Event()
        self._checker = Thread(target=self._run, name='Warning-Checker', daemon=True)

    def _run(self):
        next_check = 1
        for warning in self._warnings:
            warn = warning.check(self._job_control)
            if warn:
                self._job_control.add_warning(warn)
            next_check = min(next_check, warning.next_check(self._job_control))
        self._wait.wait(next_check)
