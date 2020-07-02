import abc
import logging
import re
from collections import namedtuple
from enum import Enum
from threading import Event, Thread

from taro import JobInfo, ExecutionStateObserver, util

log = logging.getLogger(__name__)

Warn = namedtuple('Warn', 'id params')

EXEC_TIME_WARN_REGEX = r'exec_time>(\d+)([smh])'
FILE_CONTAINS_REGEX = r'file:(.+)=~(.+)'


class WarningEvent(Enum):
    NEW_WARNING = 1
    WARNING_UPDATED = 2
    WARNING_REMOVED = 3


class JobWarningObserver(abc.ABC):

    @abc.abstractmethod
    def warning_update(self, job_info: JobInfo, warning: Warn, event: WarningEvent):
        """This method is called when there is a new warning event."""


class WarningCheck(abc.ABC):

    @abc.abstractmethod
    def warning_id(self):
        """
        :return: identifier of the warning
        """

    @abc.abstractmethod
    def check(self, job_instance, last_check: bool) -> bool:
        """
        Check warning condition.

        :param job_instance: checked job
        :param last_check: True if no more checks are scheduled
        :return: True if warning or False otherwise
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
        self._warnings = list(*warning)  # TODO check no duplicated warning ID
        self._run_condition = Event()
        self._checker = Thread(target=self.run, name='Warning-Checker')
        self._stop = False

    def state_update(self, job_info: JobInfo):
        if job_info.state == job_info.lifecycle.first_executing_state():
            self._checker.start()  # Execution started
        if job_info.state.is_terminal():
            self._stop = True
            self._run_condition.set()

    def run(self):
        log.debug("event=[warn_checking_started]")

        while True:
            next_check = -1 if self._stop else 1
            for warning in list(self._warnings):
                is_warn = warning.check(self._job_control, last_check=next_check == -1)
                already_added = warning.warning_id() in (w.id for w in self._job_control.warnings)

                if is_warn and not already_added:
                    self._job_control.add_warning(Warn(warning.warning_id(), {}))  # Params needed?
                elif already_added and not is_warn:
                    self._job_control.remove_warning(warning.warning_id())

                w_next_check = warning.next_check(self._job_control)
                if w_next_check <= 0:
                    self._warnings.remove(warning)
                else:
                    next_check = min(next_check, w_next_check)

            if next_check >= 0:
                self._run_condition.wait(next_check)
            else:
                break

        log.debug("event=[warn_checking_ended]")


def start_checking(job_control, *warning):
    checking = _WarnChecking(job_control, warning)
    job_control.add_state_observer(checking)


def setup_checking(job_control, *warning: str):
    warns = []
    for w_str in warning:
        try:
            warns.append(create_warn_from_str(w_str))
        except Exception:
            log.exception('event=[warning_creation_error]')

    start_checking(job_control, *warns)


def create_warn_from_str(val) -> WarningCheck:
    m = re.compile(EXEC_TIME_WARN_REGEX).match(val.replace(" ", "").rstrip())
    if m:
        value = int(m.group(1))
        unit = m.group(2)
        if unit == 'm':
            value *= 60
        if unit == 'h':
            value *= 60 * 60

        return ExecTimeWarning(m.group(0), value)

    m = re.compile(FILE_CONTAINS_REGEX).match(val.replace(" ", "").rstrip())
    if m:
        file = m.group(1)
        regex = m.group(2)
        return FileContainsWarning(m.group(0), file, regex)

    else:
        raise ValueError('Unknown warning: ' + val)


class ExecTimeWarning(WarningCheck):

    def __init__(self, w_id, time: float):
        self.id = w_id
        self.time = time
        self.warn = False

    def warning_id(self):
        return self.id

    def remaining_time_sec(self, job_instance):
        started = job_instance.lifecycle.execution_started()
        if not started:
            return None
        exec_time = util.utc_now() - job_instance.lifecycle.execution_started()
        return self.time - exec_time.total_seconds()

    def check(self, job_instance, last_check: bool):
        remaining_time = self.remaining_time_sec(job_instance)
        if not remaining_time or remaining_time >= 0:
            return False

        self.warn = True
        return True

    def next_check(self, job_instance) -> float:
        remaining_time = self.remaining_time_sec(job_instance)
        if not remaining_time:
            return self.time + 1.0
        if remaining_time > 0:
            return remaining_time + 0.5
        if self.warn:
            return -1.0
        else:
            return 1.0

    def __repr__(self):
        return "{}({!r}, {!r})".format(
            self.__class__.__name__, self.id, self.time)


class FileContainsWarning(WarningCheck):

    def __init__(self, w_id, file_path, regex):
        self.id = w_id
        self.file_path = file_path
        self.regex = re.compile(regex)
        self.file = None
        self.warn = False

    def warning_id(self):
        return self.id

    def check(self, job_instance, last_check: bool) -> bool:
        if not self.file:
            try:
                self.file = open(self.file_path, 'r')
            except FileNotFoundError:
                return False

        while True:
            new = self.file.readline()
            # Once all lines are read this just returns ''
            # until the file changes and a new line appears

            if not new:
                break

            if self.regex.search(new):
                self.warn = True
                break

        if last_check or self.warn:
            self.file.close()

        return self.warn

    def next_check(self, job_instance) -> float:
        if self.warn:
            return -1

        return 3.0  # Check at least every 3 seconds
