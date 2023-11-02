from threading import Thread

import pytest

from tarotools.taro.err import InvalidStateError
from tarotools.taro.jobs.coordination import ApprovalPhase
from tarotools.taro.run import Phaser, StandardPhase, TerminationStatus, PhaseStep, Phase, RunState, WaitWrapperStep, \
    FailedRun, RunError


class ExecTestPhase(PhaseStep):

    def __init__(self, name):
        super().__init__(Phase(name, RunState.EXECUTING))
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
    phaser = Phaser([WaitWrapperStep(ApprovalPhase('APPROVAL')), ExecTestPhase('EXEC')])
    return phaser


def test_run_with_approval(sut_approve):
    sut_approve.prime()
    run_thread = Thread(target=sut_approve.run)
    run_thread.start()
    # The below code will be released once the run starts pending in the approval phase
    wait_wrapper = sut_approve.get_typed_phase_step(WaitWrapperStep, 'APPROVAL')

    wait_wrapper.wait(1)
    assert sut_approve.lifecycle.phase == Phase('APPROVAL', RunState.PENDING)
    assert sut_approve.lifecycle.state == RunState.PENDING

    wait_wrapper.wrapped_step.approve()
    run_thread.join(1)
    assert (sut_approve.lifecycle.phases ==
            [
                StandardPhase.INIT.value,
                Phase('APPROVAL', RunState.PENDING),
                Phase('EXEC', RunState.EXECUTING),
                StandardPhase.TERMINAL.value
            ])


def test_post_prime(sut):
    sut.prime()
    assert sut.lifecycle.phase == StandardPhase.INIT.value
    assert sut.lifecycle.state == RunState.CREATED


def test_empty_phaser():
    empty = Phaser([])
    empty.prime()
    assert empty.lifecycle.phases == [StandardPhase.INIT.value]

    empty.run()
    assert empty.lifecycle.phases == [StandardPhase.INIT.value, StandardPhase.TERMINAL.value]
    assert empty.termination_status == TerminationStatus.COMPLETED
    assert empty.lifecycle.termination_status == TerminationStatus.COMPLETED


def test_stop_before_prime(sut):
    sut.stop()
    assert sut.lifecycle.phases == [StandardPhase.TERMINAL.value]
    assert sut.termination_status == TerminationStatus.STOPPED


def test_stop_before_run(sut):
    sut.prime()
    sut.stop()
    assert sut.lifecycle.phases == [StandardPhase.INIT.value, StandardPhase.TERMINAL.value]
    assert sut.termination_status == TerminationStatus.STOPPED


def test_stop_in_run(sut_approve):
    sut_approve.prime()
    run_thread = Thread(target=sut_approve.run)
    run_thread.start()
    # The below code will be released once the run starts pending in the approval phase
    sut_approve.get_typed_phase_step(WaitWrapperStep, 'APPROVAL').wait(1)

    sut_approve.stop()
    run_thread.join(1)  # Let the run end
    assert (sut_approve.lifecycle.phases ==
            [StandardPhase.INIT.value, Phase('APPROVAL', RunState.PENDING), StandardPhase.TERMINAL.value])
    assert sut_approve.termination_status == TerminationStatus.CANCELLED


def test_premature_termination(sut):
    sut.get_typed_phase_step(ExecTestPhase, 'EXEC1').fail = True
    sut.prime()
    sut.run()

    assert sut.termination_status == TerminationStatus.FAILED
    assert sut.lifecycle.termination_status == TerminationStatus.FAILED
    assert (sut.lifecycle.phases ==
            [
                StandardPhase.INIT.value,
                Phase('EXEC1', RunState.EXECUTING),
                StandardPhase.TERMINAL.value
            ])


def test_transition_hook(sut):
    transitions = []

    def hook(*args):
        transitions.append(args)

    sut.transition_hook = hook

    sut.prime()

    assert len(transitions) == 1
    assert transitions[0] == (StandardPhase.NONE.value, StandardPhase.INIT.value, 1)

    sut.run()

    assert len(transitions) == 4


def test_failed_run_exception(sut):
    failed_run = FailedRun('FaultType', 'reason')
    sut.get_typed_phase_step(ExecTestPhase, 'EXEC1').failed_run = failed_run
    sut.prime()
    sut.run()

    assert sut.termination_status == TerminationStatus.FAILED
    assert sut.lifecycle.termination_status == TerminationStatus.FAILED
    assert (sut.lifecycle.phases ==
            [
                StandardPhase.INIT.value,
                Phase('EXEC1', RunState.EXECUTING),
                StandardPhase.TERMINAL.value
            ])

    assert sut.run_failure == failed_run.fault


def test_exception(sut):
    exc = InvalidStateError('reason')
    sut.get_typed_phase_step(ExecTestPhase, 'EXEC1').exception = exc
    sut.prime()
    sut.run()

    assert sut.termination_status == TerminationStatus.ERROR
    assert sut.lifecycle.termination_status == TerminationStatus.ERROR
    assert (sut.lifecycle.phases ==
            [
                StandardPhase.INIT.value,
                Phase('EXEC1', RunState.EXECUTING),
                StandardPhase.TERMINAL.value
            ])

    assert sut.run_error == RunError('InvalidStateError', 'reason')


def test_interruption(sut):
    sut.get_typed_phase_step(ExecTestPhase, 'EXEC1').exception = KeyboardInterrupt
    sut.prime()

    exc_passed = False
    try:
        sut.run()
    except KeyboardInterrupt:
        exc_passed = True

    assert sut.termination_status == TerminationStatus.INTERRUPTED
    assert sut.lifecycle.termination_status == TerminationStatus.INTERRUPTED
    assert exc_passed
