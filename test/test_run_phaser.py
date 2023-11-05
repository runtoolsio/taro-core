from threading import Thread

import pytest

from tarotools.taro.err import InvalidStateError
from tarotools.taro.jobs.coordination import ApprovalPhase
from tarotools.taro.run import Phaser, StandardPhaseNames, TerminationStatus, Phase, RunState, WaitWrapperPhase, \
    FailedRun, RunError


class ExecTestPhase(Phase):

    def __init__(self, name):
        super().__init__(name, RunState.EXECUTING)
        self.fail = False
        self.failed_run = None
        self.exception = None

    @property
    def stop_status(self):
        return TerminationStatus.STOPPED

    def run(self):
        if self.exception:
            raise self.exception
        if self.failed_run:
            raise self.failed_run

        return TerminationStatus.FAILED if self.fail else TerminationStatus.NONE

    def stop(self):
        pass


@pytest.fixture
def sut():
    phaser = Phaser([ExecTestPhase('EXEC1'), ExecTestPhase('EXEC2')])
    return phaser


@pytest.fixture
def sut_approve():
    phaser = Phaser([WaitWrapperPhase(ApprovalPhase('APPROVAL')), ExecTestPhase('EXEC')])
    return phaser


def test_run_with_approval(sut_approve):
    sut_approve.prime()
    run_thread = Thread(target=sut_approve.run)
    run_thread.start()
    # The below code will be released once the run starts pending in the approval phase
    wait_wrapper = sut_approve.get_typed_phase(WaitWrapperPhase, 'APPROVAL')

    wait_wrapper.wait(1)
    snapshot = sut_approve.create_run_snapshot()
    assert snapshot.lifecycle.current_phase == 'APPROVAL'
    assert snapshot.lifecycle.run_state == RunState.PENDING

    wait_wrapper.wrapped_step.approve()
    run_thread.join(1)
    assert (sut_approve.create_run_snapshot().lifecycle.phases ==
            [
                StandardPhaseNames.INIT,
                Phase('APPROVAL', RunState.PENDING),
                Phase('EXEC', RunState.EXECUTING),
                StandardPhaseNames.TERMINAL
            ])


def test_post_prime(sut):
    sut.prime()

    snapshot = sut.create_run_snapshot()
    assert snapshot.lifecycle.current_phase == StandardPhaseNames.INIT
    assert snapshot.lifecycle.run_state == RunState.CREATED


def test_empty_phaser():
    empty = Phaser([])
    empty.prime()
    assert empty.create_run_snapshot().lifecycle.phases == [StandardPhaseNames.INIT]

    empty.run()

    snapshot = empty.create_run_snapshot()
    assert snapshot.lifecycle.phases == [StandardPhaseNames.INIT, StandardPhaseNames.TERMINAL]
    assert snapshot.termination_status == TerminationStatus.COMPLETED


def test_stop_before_prime(sut):
    sut.stop()

    snapshot = sut.create_run_snapshot()
    assert snapshot.lifecycle.phases == [StandardPhaseNames.TERMINAL]
    assert snapshot.termination_status == TerminationStatus.STOPPED


def test_stop_before_run(sut):
    sut.prime()
    sut.stop()

    snapshot = sut.create_run_snapshot()
    assert snapshot.lifecycle.phases == [StandardPhaseNames.INIT, StandardPhaseNames.TERMINAL]
    assert snapshot.termination_status == TerminationStatus.STOPPED


def test_stop_in_run(sut_approve):
    sut_approve.prime()
    run_thread = Thread(target=sut_approve.run)
    run_thread.start()
    # The below code will be released once the run starts pending in the approval phase
    sut_approve.get_typed_phase(WaitWrapperPhase, 'APPROVAL').wait(1)

    sut_approve.stop()
    run_thread.join(1)  # Let the run end

    snapshot = sut_approve.create_run_snapshot()
    assert (snapshot.lifecycle.phases == [StandardPhaseNames.INIT, 'APPROVAL', StandardPhaseNames.TERMINAL])
    assert snapshot.termination_status == TerminationStatus.CANCELLED


def test_premature_termination(sut):
    sut.get_typed_phase(ExecTestPhase, 'EXEC1').fail = True
    sut.prime()
    sut.run()

    snapshot = sut.create_run_snapshot()
    assert snapshot.termination_status == TerminationStatus.FAILED
    assert (snapshot.lifecycle.phases == [StandardPhaseNames.INIT, 'EXEC1', StandardPhaseNames.TERMINAL])


def test_transition_hook(sut):
    transitions = []

    def hook(*args):
        transitions.append(args)

    sut.transition_hook = hook

    sut.prime()

    assert len(transitions) == 1
    prev_run, new_run, ordinal = transitions[0]
    assert prev_run is None
    assert new_run.phase_name is StandardPhaseNames.INIT
    assert ordinal == 1

    sut.run()

    assert len(transitions) == 4


def test_failed_run_exception(sut):
    failed_run = FailedRun('FaultType', 'reason')
    sut.get_typed_phase(ExecTestPhase, 'EXEC1').failed_run = failed_run
    sut.prime()
    sut.run()

    snapshot = sut.create_run_snapshot()
    assert snapshot.termination_status == TerminationStatus.FAILED
    assert (snapshot.lifecycle.phases == [StandardPhaseNames.INIT, 'EXEC1', StandardPhaseNames.TERMINAL])

    assert snapshot.run_failure == failed_run.fault


def test_exception(sut):
    exc = InvalidStateError('reason')
    sut.get_typed_phase(ExecTestPhase, 'EXEC1').exception = exc
    sut.prime()
    sut.run()

    snapshot = sut.create_run_snapshot()
    assert snapshot.termination_status == TerminationStatus.ERROR
    assert (snapshot.lifecycle.phases == [StandardPhaseNames.INIT, 'EXEC1', StandardPhaseNames.TERMINAL])

    assert snapshot.run_error == RunError('InvalidStateError', 'reason')


def test_interruption(sut):
    sut.get_typed_phase(ExecTestPhase, 'EXEC1').exception = KeyboardInterrupt
    sut.prime()

    exc_passed = False
    try:
        sut.run()
    except KeyboardInterrupt:
        exc_passed = True

    snapshot = sut.create_run_snapshot()
    assert snapshot.termination_status == TerminationStatus.INTERRUPTED
    assert exc_passed
