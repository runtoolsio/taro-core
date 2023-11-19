from datetime import datetime, timedelta

from tarotools.taro import JobRun, util, RunnerJobInstance
from tarotools.taro.execution import ExecutingPhase
from tarotools.taro.jobs.coordination import ApprovalPhase
from tarotools.taro.jobs.instance import JobInstanceMetadata
from tarotools.taro.run import PhaseRun, TerminationInfo, Lifecycle, RunState, PhaseMetadata, Run, StandardPhaseNames, \
    Phaser, TerminationStatus, RunFailure
from tarotools.taro.test.execution import TestExecution


class AbstractBuilder:

    current_ts = datetime.utcnow().replace(microsecond=0)

    def __init__(self, job_id, run_id=None, system_params=None, user_params=None):
        instance_id = util.unique_timestamp_hex()
        run_id = run_id or instance_id
        self.metadata = JobInstanceMetadata(job_id, run_id, instance_id, system_params or {}, user_params or {})
        self.termination_info = None


class TestJobInstanceBuilder(AbstractBuilder):

    def __init__(self, job_id, run_id=None, system_params=None, user_params=None):
        super().__init__(job_id, run_id, system_params, user_params)
        self.phases = []

    def add_approval_phase(self, name=StandardPhaseNames.APPROVAL):
        self.phases.append(ApprovalPhase(name, 2))
        return self

    def add_exec_phase(self, name='EXEC'):
        self.phases.append(ExecutingPhase(name, TestExecution(wait=True)))
        return self

    def build(self):
        phaser = Phaser(self.phases, Lifecycle())
        return RunnerJobInstance(self.metadata.job_id, phaser, run_id=self.metadata.run_id, **self.metadata.user_params)


class TestJobRunBuilder(AbstractBuilder):

    def __init__(self, job_id, run_id=None, system_params=None, user_params=None):
        super().__init__(job_id, run_id, system_params, user_params)
        self.phases = []

    def add_phase(self, name, state, start=None, end=None):
        if not start:
            start = super().current_ts
            end = start + timedelta(minutes=1)

        if name != StandardPhaseNames.INIT and not self.phases:
            self.add_phase(
                StandardPhaseNames.INIT, RunState.CREATED, start - timedelta(minutes=2), start - timedelta(minutes=1))

        phase_run = PhaseRun(name, state, start, end)
        self.phases.append(phase_run)
        return self

    def with_termination_info(self, status, time, failure=None):
        self.termination_info = TerminationInfo(status, time, failure)
        return self

    def build(self):
        lifecycle = Lifecycle(*self.phases)
        run = Run((PhaseMetadata('p1', RunState.EXECUTING, {'p': 'v'}),), lifecycle, self.termination_info)
        return JobRun(self.metadata, run, None)


def ended_run(job_id, run_id='r1', *, offset_min=0, term_status=TerminationStatus.COMPLETED, created=None,
              completed=None):
    start_time = datetime.utcnow().replace(microsecond=0) + timedelta(minutes=offset_min)

    builder = TestJobRunBuilder(job_id, run_id, user_params={'name': 'value'})

    builder.add_phase(StandardPhaseNames.INIT, RunState.CREATED, created or start_time,
                      start_time + timedelta(minutes=1))
    builder.add_phase(StandardPhaseNames.APPROVAL, RunState.EXECUTING, start_time + timedelta(minutes=1),
                      start_time + timedelta(minutes=2))
    builder.add_phase(StandardPhaseNames.PROGRAM, RunState.EXECUTING, start_time + timedelta(minutes=2),
                      start_time + timedelta(minutes=3))
    builder.add_phase(StandardPhaseNames.TERMINAL, RunState.ENDED, completed or start_time + timedelta(minutes=3), None)

    failure = RunFailure('err1', 'reason') if term_status == TerminationStatus.FAILED else None
    builder.with_termination_info(term_status, start_time + timedelta(minutes=3), failure)

    return builder.build()
