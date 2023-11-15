from collections import Counter

from tarotools.taro import TerminationStatus, Flag
from tarotools.taro.execution import Phase
from tarotools.taro.jobs.criteria import IntervalCriterion, TerminationCriterion
from tarotools.taro.jobs.instance import LifecycleEvent
from tarotools.taro.test.inst import TestJobInstance


def test_interval_utc_conversion():
    c = IntervalCriterion.to_utc(LifecycleEvent.CREATED, from_val='2023-11-10T09:00+02:00', to_val=None)
    assert c.from_dt.hour == 7


def test_phase_criteria():
    inst = TestJobInstance('skipped', '', TerminationStatus.INVALID_OVERLAP)
    inst.warnings = Counter(['error_output'])
    matching1 = TerminationCriterion(flag_groups=[{Flag.SUCCESS}, {Flag.UNEXECUTED, Flag.NONSUCCESS}])
    not_matching1 = TerminationCriterion(flag_groups=[{Flag.SUCCESS}, {Flag.UNEXECUTED, Flag.ABORTED}])

    matching2 = TerminationCriterion(flag_groups=[{Flag.UNEXECUTED}], warning=True)
    not_matching2 = TerminationCriterion(flag_groups=[{Flag.UNEXECUTED}], warning=False)

    assert matching1(inst)
    assert not not_matching1(inst)
    assert matching2(inst)
    assert not not_matching2(inst)


def test_phase_criteria_phases():
    c = TerminationCriterion(phases={Phase.PENDING, Phase.QUEUED})

    assert c(TestJobInstance(phase=TerminationStatus.PENDING))
    assert c(TestJobInstance(phase=TerminationStatus.QUEUED))
    assert not c(TestJobInstance(phase=TerminationStatus.CREATED))
    assert not c(TestJobInstance(phase=TerminationStatus.RUNNING))
    assert not c(TestJobInstance(phase=TerminationStatus.INVALID_OVERLAP))

