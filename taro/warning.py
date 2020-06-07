import abc
from collections import namedtuple

from taro import JobInfo

Warn = namedtuple('Warn', 'type params')


class JobWarningObserver(abc.ABC):

    @abc.abstractmethod
    def warning_added(self, job_info: JobInfo, warning: Warn):
        """This method is called when job instance execution state is changed."""

    @abc.abstractmethod
    def warning_removed(self, job_info: JobInfo, warning: Warn):
        """This method is called when job instance execution state is changed."""
