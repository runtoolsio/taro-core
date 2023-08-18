import re
from threading import Timer
from typing import Sequence

from tarotools.taro import util
from tarotools.taro.jobs.execution import ExecutionPhase
from tarotools.taro.jobs.inst import JobInstance, JobInst, InstanceStateObserver, Warn, InstanceOutputObserver


def exec_time_exceeded(job_instance: JobInstance, warning_name: str, time: float):
    job_instance.add_state_observer(_ExecTimeWarning(job_instance, warning_name, time))


def output_matches(job_instance: JobInstance, warning_name: str, regex: str):
    job_instance.add_output_observer(_OutputMatchesWarning(job_instance, warning_name, regex))


def register(job_instance: JobInstance, *, warn_times: Sequence[str] = (), warn_outputs: Sequence[str] = ()):
    for warn_time in warn_times:
        time = util.parse_duration_to_sec(warn_time)
        exec_time_exceeded(job_instance, f"exec_time>{time}s", time)

    for warn_output in warn_outputs:
        output_matches(job_instance, f"output=~{warn_output}", warn_output)


class _ExecTimeWarning(InstanceStateObserver):

    def __init__(self, job_instance, name, time: float):
        self.job_instance = job_instance
        self.name = name
        self.time = time
        self.timer = None

    def instance_state_update(self, job_inst: JobInst, previous_state, new_state, changed):
        if new_state.in_phase(ExecutionPhase.EXECUTING):
            assert self.timer is None
            self.timer = Timer(self.time, self._check)
            self.timer.start()
        elif new_state.in_phase(ExecutionPhase.TERMINAL) and self.timer is not None:
            self.timer.cancel()

    def _check(self):
        if not self.job_instance.lifecycle.state.in_phase(ExecutionPhase.TERMINAL):
            warn = Warn(self.name, {'exceeded_sec': self.time})
            self.job_instance.add_warning(warn)

    def __repr__(self):
        return "{}({!r}, {!r}, {!r})".format(
            self.__class__.__name__, self.job_instance, self.name, self.time)


class _OutputMatchesWarning(InstanceOutputObserver):

    def __init__(self, job_instance, w_id, regex):
        self.job_instance = job_instance
        self.id = w_id
        self.regex = re.compile(regex)

    def instance_output_update(self, _, output, is_error):
        m = self.regex.search(output)
        if m:
            warn = Warn(self.id, {'matches': output})
            self.job_instance.add_warning(warn)