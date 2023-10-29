import datetime

import pytest

from tarotools.taro.run import PhaseTransition, Phase, StandardPhase, RunState, Lifecycle


@pytest.fixture
def sut():
    base = datetime.datetime(2023, 1, 1)

    # Initial transition
    init_transition = PhaseTransition(StandardPhase.INIT.value, base)
    lifecycle = Lifecycle(init_transition)

    # Adding more transitions
    # 10 minutes after initialization, it goes to PENDING state
    lifecycle.add_transition(PhaseTransition(Phase("PENDING", RunState.PENDING), base + datetime.timedelta(minutes=10)))

    # 20 minutes after initialization, it goes to EXECUTING state
    lifecycle.add_transition(
        PhaseTransition(Phase("EXECUTING", RunState.EXECUTING), base + datetime.timedelta(minutes=20)))

    # 50 minutes after initialization, it terminates
    lifecycle.add_transition(PhaseTransition(StandardPhase.TERMINAL.value, base + datetime.timedelta(minutes=50)))

    return lifecycle


def test_phases(sut):
    assert sut.phases == [
        StandardPhase.INIT.value,
        Phase("PENDING", RunState.PENDING),
        Phase("EXECUTING", RunState.EXECUTING),
        StandardPhase.TERMINAL.value
    ]


def test_phase_run(sut):
    init_phase_run = sut.phase_run(StandardPhase.INIT.value)
    assert init_phase_run.started_at == datetime.datetime(2023, 1, 1)
    assert init_phase_run.ended_at == datetime.datetime(2023, 1, 1, 0, 10)
    assert init_phase_run.execution_time == datetime.timedelta(minutes=10)


def test_execution_time(sut):
    # 50min - 20min based on create_sut()
    assert sut.total_executing_time == datetime.timedelta(minutes=30)
