import datetime

import pytest

from tarotools.taro.run import PhaseTransition, Phase, StandardPhase, RunState, Lifecycle
from tarotools.taro.util import utc_now

PENDING = Phase("PENDING", RunState.PENDING)
EXECUTING = Phase("EXECUTING", RunState.EXECUTING)


@pytest.fixture
def sut() -> Lifecycle:
    # Initial transition
    base = datetime.datetime(2023, 1, 1)
    init_transition = PhaseTransition(StandardPhase.INIT.value, base)
    lifecycle = Lifecycle(init_transition)

    # 10 minutes after initialization, it goes to PENDING state
    lifecycle.add_transition(PhaseTransition(PENDING, base + datetime.timedelta(minutes=10)))
    # 20 minutes after initialization, it goes to EXECUTING state
    lifecycle.add_transition(PhaseTransition(EXECUTING, base + datetime.timedelta(minutes=20)))
    # 50 minutes after initialization, it terminates
    lifecycle.add_transition(PhaseTransition(StandardPhase.TERMINAL.value, base + datetime.timedelta(minutes=50)))

    return lifecycle


def test_phases(sut):
    assert sut.phases == [
        StandardPhase.INIT.value,
        PENDING,
        EXECUTING,
        StandardPhase.TERMINAL.value
    ]
    assert sut.phase == StandardPhase.TERMINAL.value
    assert sut.phase_count == 4


def test_ordinal(sut):
    assert sut.get_ordinal(PENDING) == 2


def test_transitions(sut):
    assert sut.transitioned_at(EXECUTING) == datetime.datetime(2023, 1, 1, 0, 20)
    assert sut.last_transition_at == datetime.datetime(2023, 1, 1, 0, 50)


def test_states(sut):
    assert sut.state_first_at(RunState.EXECUTING) == datetime.datetime(2023, 1, 1, 0, 20)
    assert sut.state_last_at(RunState.ENDED) == datetime.datetime(2023, 1, 1, 0, 50)
    assert sut.contains_state(RunState.CREATED)
    assert not sut.contains_state(RunState.IN_QUEUE)
    assert sut.created_at == datetime.datetime(2023, 1, 1, 0, 0)
    assert sut.executed_at == datetime.datetime(2023, 1, 1, 0, 20)
    assert sut.ended_at == datetime.datetime(2023, 1, 1, 0, 50)


def test_current_and_previous_phase(sut):
    assert sut.phase == StandardPhase.TERMINAL.value
    assert sut.previous_phase == EXECUTING


def test_phase_run(sut):
    init_phase_run = sut.phase_run(StandardPhase.INIT.value)
    assert init_phase_run.started_at == datetime.datetime(2023, 1, 1)
    assert init_phase_run.ended_at == datetime.datetime(2023, 1, 1, 0, 10)
    assert init_phase_run.execution_time == datetime.timedelta(minutes=10)


def test_termination(sut):
    assert sut.is_ended
    assert not Lifecycle(PhaseTransition(StandardPhase.INIT.value, utc_now())).is_ended


def test_execution_time(sut):
    # 50min - 20min based on create_sut()
    assert sut.total_executing_time == datetime.timedelta(minutes=30)


def test_phases_between(sut):
    assert sut.phases_between(PENDING, EXECUTING) == [PENDING, EXECUTING]
    assert (sut.phases_between(PENDING, StandardPhase.TERMINAL.value)
            == [PENDING, EXECUTING, StandardPhase.TERMINAL.value])
    assert sut.phases_between(PENDING, PENDING) == [PENDING]
    assert sut.phases_between(EXECUTING, PENDING) == []
    assert sut.phases_between(PENDING, Phase('Not contained', RunState.NONE)) == []
    assert sut.phases_between(Phase('Not contained', RunState.NONE), PENDING) == []
