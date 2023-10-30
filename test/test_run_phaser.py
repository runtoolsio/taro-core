from threading import Thread

import pytest

from tarotools.taro.jobs.coordination import ApprovalPhase
from tarotools.taro.run import Phaser, StandardPhase, TerminationStatus, PhaseStep, Phase, RunState, WaitWrapperStep


class ExecTestPhase(PhaseStep):

    @property
    def phase(self):
        return Phase('EXEC', RunState.EXECUTING)

    def run(self):
        pass

    def stop(self):
        pass

    @property
    def stop_status(self):
        return TerminationStatus.STOPPED


@pytest.fixture
def sut():
    phaser = Phaser([WaitWrapperStep(ApprovalPhase('APPROVAL')), ExecTestPhase()])
    return phaser


def test_empty_phaser():
    empty = Phaser([])
    empty.prime()
    assert empty.lifecycle.phases == [StandardPhase.INIT.value]

    empty.run()
    assert empty.lifecycle.phases == [StandardPhase.INIT.value, StandardPhase.TERMINAL.value]
    assert empty.termination_status == TerminationStatus.COMPLETED
    assert empty.lifecycle.termination_status == TerminationStatus.COMPLETED


def test_stop_before_run(sut):
    sut.prime()
    sut.stop()
    assert sut.lifecycle.phases == [StandardPhase.INIT.value, StandardPhase.TERMINAL.value]
    assert sut.termination_status == TerminationStatus.STOPPED


def test_stop_before_prime(sut):
    sut.stop()
    assert sut.lifecycle.phases == [StandardPhase.TERMINAL.value]
    assert sut.termination_status == TerminationStatus.STOPPED


def test_stop_after_run(sut):
    sut.prime()
    run_thread = Thread(target=sut.run)
    run_thread.start()
    # The below code will be released once the run starts pending in the approval phase
    sut.get_typed_phase_step(WaitWrapperStep, 'APPROVAL').wait(1)

    sut.stop()
    run_thread.join(1)  # Let the run end
    assert (sut.lifecycle.phases ==
            [StandardPhase.INIT.value, Phase('APPROVAL', RunState.PENDING), StandardPhase.TERMINAL.value])
    assert sut.termination_status == TerminationStatus.CANCELLED
