from tarotools.taro.jobs.criteria import IntervalCriterion, TerminationCriterion
from tarotools.taro.run import RunState, TerminationStatus, Flag, TerminationInfo

from tarotools.taro.util import utc_now


def test_interval_utc_conversion():
    c = IntervalCriterion.to_utc(RunState.CREATED, from_val='2023-11-10T09:00+02:00', to_val=None)
    assert c.from_dt.hour == 7


def test_termination_criteria():
    matching = TerminationCriterion(status_flag_groups=[{Flag.SUCCESS}, {Flag.UNEXECUTED, Flag.NONSUCCESS}])
    not_matching = TerminationCriterion(status_flag_groups=[{Flag.SUCCESS}, {Flag.UNEXECUTED, Flag.ABORTED}])

    tested_info = TerminationInfo(TerminationStatus.INVALID_OVERLAP, utc_now())
    assert matching(tested_info)
    assert not not_matching(tested_info)
