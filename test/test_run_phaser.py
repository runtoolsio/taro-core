from threading import Thread, Event
from typing import Optional

import pytest

from tarotools.taro.common import InvalidStateError
from tarotools.taro.run import Phaser, PhaseNames, TerminationStatus, Phase, RunState, WaitWrapperPhase, \
    FailedRun, RunError, TerminateRun


class TestPhase(Phase):

    def __init__(self, name, wait=False):
        super().__init__(name, RunState.PENDING if wait else RunState.EXECUTING)
        self.fail = False
        self.failed_run = None
        self.exception = None
        self.wait: Optional[Event] = Event() if wait else None

    @property
    def stop_status(self):
        if self.wait:
            return TerminationStatus.CANCELLED
        else:
            return TerminationStatus.STOPPED

    def run(self, run_ctx):
        if self.wait:
            self.wait.wait(2)
        if self.exception:
            raise self.exception
        if self.failed_run:
            raise self.failed_run
        if self.fail:
            raise TerminateRun(TerminationStatus.FAILED)

    def stop(self):
        if self.wait:
            self.wait.set()


@pytest.fixture
def sut():
    phaser = Phaser([TestPhase('EXEC1'), TestPhase('EXEC2')])
    return phaser


@pytest.fixture
def sut_approve():
    phaser = Phaser([WaitWrapperPhase(TestPhase('APPROVAL', wait=True)), TestPhase('EXEC')])
    return phaser


def test_run_with_approval(sut_approve):
    sut_approve.prime()
    run_thread = Thread(target=sut_approve.run)
    run_thread.start()
    # The below code will be released once the run starts pending in the approval phase
    wait_wrapper = sut_approve.get_typed_phase(WaitWrapperPhase, 'APPROVAL')

    wait_wrapper.wait(1)
    snapshot = sut_approve.run_info()
    assert snapshot.lifecycle.current_phase_name == 'APPROVAL'
    assert snapshot.lifecycle.run_state == RunState.PENDING

    wait_wrapper.wrapped_phase.wait.set()
    run_thread.join(1)
    assert (sut_approve.run_info().lifecycle.phases ==
            [PhaseNames.INIT, 'APPROVAL', 'EXEC', PhaseNames.TERMINAL])


def test_post_prime(sut):
    sut.prime()

    snapshot = sut.run_info()
    assert snapshot.lifecycle.current_phase_name == PhaseNames.INIT
    assert snapshot.lifecycle.run_state == RunState.CREATED


def test_empty_phaser():
    empty = Phaser([])
    empty.prime()
    assert empty.run_info().lifecycle.phases == [PhaseNames.INIT]

    empty.run()

    snapshot = empty.run_info()
    assert snapshot.lifecycle.phases == [PhaseNames.INIT, PhaseNames.TERMINAL]
    assert snapshot.termination.status == TerminationStatus.COMPLETED


def test_stop_before_prime(sut):
    sut.stop()

    snapshot = sut.run_info()
    assert snapshot.lifecycle.phases == [PhaseNames.TERMINAL]
    assert snapshot.termination.status == TerminationStatus.STOPPED


def test_stop_before_run(sut):
    sut.prime()
    sut.stop()

    snapshot = sut.run_info()
    assert snapshot.lifecycle.phases == [PhaseNames.INIT, PhaseNames.TERMINAL]
    assert snapshot.termination.status == TerminationStatus.STOPPED


def test_stop_in_run(sut_approve):
    sut_approve.prime()
    run_thread = Thread(target=sut_approve.run)
    run_thread.start()
    # The below code will be released once the run starts pending in the approval phase
    sut_approve.get_typed_phase(WaitWrapperPhase, 'APPROVAL').wait(1)

    sut_approve.stop()
    run_thread.join(1)  # Let the run end

    run = sut_approve.run_info()
    assert (run.lifecycle.phases == [PhaseNames.INIT, 'APPROVAL', PhaseNames.TERMINAL])
    assert run.termination.status == TerminationStatus.CANCELLED


def test_premature_termination(sut):
    sut.get_typed_phase(TestPhase, 'EXEC1').fail = True
    sut.prime()
    sut.run()

    run = sut.run_info()
    assert run.termination.status == TerminationStatus.FAILED
    assert (run.lifecycle.phases == [PhaseNames.INIT, 'EXEC1', PhaseNames.TERMINAL])


def test_transition_hook(sut):
    transitions = []

    def hook(*args):
        transitions.append(args)

    sut.transition_hook = hook

    sut.prime()

    assert len(transitions) == 1
    prev_run, new_run, ordinal = transitions[0]
    assert not prev_run
    assert new_run.phase_name is PhaseNames.INIT
    assert ordinal == 1

    sut.run()

    assert len(transitions) == 4


def test_failed_run_exception(sut):
    failed_run = FailedRun('FaultType', 'reason')
    sut.get_typed_phase(TestPhase, 'EXEC1').failed_run = failed_run
    sut.prime()
    sut.run()

    snapshot = sut.run_info()
    assert snapshot.termination.status == TerminationStatus.FAILED
    assert (snapshot.lifecycle.phases == [PhaseNames.INIT, 'EXEC1', PhaseNames.TERMINAL])

    assert snapshot.termination.failure == failed_run.fault


def test_exception(sut):
    exc = InvalidStateError('reason')
    sut.get_typed_phase(TestPhase, 'EXEC1').exception = exc
    sut.prime()
    sut.run()

    snapshot = sut.run_info()
    assert snapshot.termination.status == TerminationStatus.ERROR
    assert (snapshot.lifecycle.phases == [PhaseNames.INIT, 'EXEC1', PhaseNames.TERMINAL])

    assert snapshot.termination.error == RunError('InvalidStateError', 'reason')


def test_interruption(sut):
    sut.get_typed_phase(TestPhase, 'EXEC1').exception = KeyboardInterrupt
    sut.prime()

    exc_passed = False
    try:
        sut.run()
    except KeyboardInterrupt:
        exc_passed = True

    snapshot = sut.run_info()
    assert snapshot.termination.status == TerminationStatus.INTERRUPTED
    assert exc_passed
