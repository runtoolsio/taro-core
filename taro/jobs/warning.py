import re
from threading import Timer
from typing import Optional

from taro.jobs.job import JobInstance, JobInfo, ExecutionStateObserver, Warn, JobOutputObserver


def exec_time_exceeded(job_instance: JobInstance, warning_name: str, time: float):
    job_instance.add_state_observer(_ExecTimeWarning(job_instance, warning_name, time))


def output_matches(job_instance: JobInstance, warning_name: str, regex: str):
    job_instance.add_output_observer(_OutputMatchesWarning(job_instance, warning_name, regex))


class _ExecTimeWarning(ExecutionStateObserver):

    def __init__(self, job_instance, name, time: float):
        self.job_instance = job_instance
        self.name = name
        self.time = time
        self.timer = None

    def state_update(self, job_info: JobInfo):
        if job_info.state.is_executing():
            assert self.timer is None
            self.timer = Timer(self.time, self._check)
            self.timer.start()
        elif job_info.state.is_terminal() and self.timer is not None:
            self.timer.cancel()

    def _check(self):
        if not self.job_instance.lifecycle.state().is_terminal():
            warn = Warn(self.name, {'exceeded_sec': self.time})
            self.job_instance.add_warning(warn)

    def __repr__(self):
        return "{}({!r}, {!r}, {!r})".format(
            self.__class__.__name__, self.job_instance, self.name, self.time)


class _OutputMatchesWarning(JobOutputObserver):

    def __init__(self, job_instance, w_id, regex):
        self.job_instance = job_instance
        self.id = w_id
        self.regex = re.compile(regex)

    def output_update(self, _, output):
        m = self.regex.search(output)
        if m:
            warn = Warn(self.id, {'matches': output})
            self.job_instance.add_warning(warn)


# TODO complete redesign
class _FileLineMatchesWarning:

    def __init__(self, w_id, file_path, regex):
        self.id = w_id
        self.file_path = file_path
        self.regex = re.compile(regex)
        self.file = None
        self.warn = False

    def check(self, job_info, last_check: bool) -> Optional[Warn]:
        if not self.file:
            try:
                self.file = open(self.file_path, 'r')
            except FileNotFoundError:
                return None

        while True:
            new = self.file.readline()
            # Once all lines are read this just returns '' until the file changes and a new line appears

            if not new:
                break

            m = self.regex.search(new)
            if m:
                self.warn = Warn(self.id, {'match': m[0]})
                break

        if last_check or self.warn:
            self.file.close()

        return self.warn

    def next_check(self, job_info) -> float:
        if self.warn:
            return -1

        return 3.0  # Check at least every 3 seconds
