import abc
import logging
from collections import namedtuple
from threading import Event, Thread

from taro import JobInfo, ExecutionStateObserver, util

log = logging.getLogger(__name__)

Warn = namedtuple('Warn', 'type params')


class JobWarningObserver(abc.ABC):

    @abc.abstractmethod
    def warning_added(self, job_info: JobInfo, warning: Warn):
        """This method is called when job instance execution state is changed."""

    @abc.abstractmethod
    def warning_removed(self, job_info: JobInfo, warning: Warn):
        """This method is called when job instance execution state is changed."""


class WarningCheck(abc.ABC):

    @abc.abstractmethod
    def warning_type(self):
        """
        :return: type of warning
        """

    @abc.abstractmethod
    def check(self, job_instance):
        """
        Check warning condition and return Warn object if the warning condition is met otherwise return None.

        :param job_instance: checked job
        :return: warning or None
        """

    @abc.abstractmethod
    def next_check(self, job_instance) -> float:
        """
        Returns maximum time in seconds after next check must be performed.
        However next check can be performed anytime sooner than after then interval specified by this method.

        :param job_instance: checked job
        :return: next latest check
        """


class _WarnChecking(ExecutionStateObserver):

    def __init__(self, job_control, *warning):
        self._job_control = job_control
        self._warnings = list(*warning)  # TODO check no duplicated warning type
        self._run_condition = Event()
        self._checker = Thread(target=self.run, name='Warning-Checker', daemon=True)
        self._started = False

    def state_update(self, job_info: JobInfo):
        if not self._started and not self._run_condition.is_set() and job_info.state.is_executing():
            self._started = True
            self._checker.start()
        if self._started and job_info.state.is_terminal():
            self._run_condition.set()

    def run(self):
        log.debug("event=[warn_checking_started]")

        while not self._run_condition.is_set():
            next_check = 1
            for warning in list(self._warnings):
                warn = warning.check(self._job_control)
                if warn:
                    self._job_control.add_warning(warn)
                elif warning.warning_type() in (w.type for w in self._job_control.warnings):
                    self._job_control.remove_warning(warning.warning_type())

                w_next_check = warning.next_check(self._job_control)
                if w_next_check <= 0:
                    self._warnings.remove(warning)
                else:
                    next_check = min(next_check, w_next_check)

            self._run_condition.wait(next_check)

        log.debug("event=[warn_checking_ended]")


def start_checking(job_control, *warning):
    checking = _WarnChecking(job_control, warning)
    job_control.add_state_observer(checking)


class ExecTimeWarning(WarningCheck):

    def __init__(self, time: int):
        self.time = time
        self.warn = None

    def warning_type(self):
        return 'exec_time'

    def remaining_time_sec(self, job_instance):
        started = job_instance.lifecycle.execution_started()
        if not started:
            return None
        exec_time = util.utc_now() - job_instance.lifecycle.execution_started()
        return self.time - exec_time.total_seconds()

    def check(self, job_instance):
        remaining_time = self.remaining_time_sec(job_instance)
        if not remaining_time or remaining_time >= 0:
            return None

        self.warn = Warn(self.warning_type(), {'warn_time_sec': self.time})
        return self.warn

    def next_check(self, job_instance) -> float:
        remaining_time = self.remaining_time_sec(job_instance)
        if not remaining_time:
            return self.time + 1.0
        if remaining_time > 0:
            return remaining_time + 1.0
        if self.warn:
            return -1.0
        else:
            return 1.0
