import sqlite3
from datetime import datetime as dt
from datetime import timedelta

import pytest

from taro import JobInfo, JobInstanceID, ExecutionLifecycle, ExecutionState, ExecutionError, util
from taro.jobs.db.sqlite import SQLite
from taro.jobs.job import parse_criteria, InstanceMatchingCriteria, IntervalCriteria, LifecycleEvent, StateCriteria, \
    JobInstanceMetadata
from taro.jobs.track import MutableTrackedTask
from taro.util import utc_now, parse_iso8601_duration, MatchingStrategy


@pytest.fixture
def sut():
    db_con = sqlite3.connect(':memory:')
    sqlite_ = SQLite(db_con)
    sqlite_.check_tables_exist()
    yield sqlite_
    sqlite_.close()


def test_store_and_fetch(sut):
    metadata = JobInstanceMetadata(JobInstanceID('j1', 'i1'), (('p1', 'v1'),), {'u1': 'v2'})
    lifecycle = ExecutionLifecycle((ExecutionState.CREATED, utc_now()))
    tracking = MutableTrackedTask('task1')
    error = ExecutionError('e1', ExecutionState.ERROR)
    j1 = JobInfo(metadata, lifecycle, tracking, 's1', 'e1', {'w': 1}, error)

    sut.store_instances(j1)
    jobs = sut.read_instances()

    assert j1 == jobs[0]


def j(c, instance=None, *, sec=0, created=utc_now(), completed=utc_now(), warnings=None):
    metadata = JobInstanceMetadata(JobInstanceID(f"j{c}", instance or util.unique_timestamp_hex()), (), {}, None)
    lifecycle = ExecutionLifecycle(
        (ExecutionState.CREATED, created + timedelta(seconds=sec)),
        (ExecutionState.COMPLETED, completed + timedelta(seconds=sec)))
    return JobInfo(metadata, lifecycle, None, None, None, warnings, None)


def test_last(sut):
    sut.store_instances(j(1), j(2), j(1), j(3), j(2))

    jobs = sut.read_instances(last=True)
    assert len(jobs) == 3
    assert {job.job_id for job in jobs} == {'j1', 'j2', 'j3'}


def test_sort(sut):
    sut.store_instances(j(1), j(2, sec=1), j(3, sec=-1))

    jobs = sut.read_instances()
    assert jobs.job_ids == ['j3', 'j1', 'j2']

    jobs = sut.read_instances(asc=False)
    assert jobs.job_ids == ['j2', 'j1', 'j3']


def test_limit(sut):
    sut.store_instances(j(1), j(2, sec=1), j(3, sec=-1))

    jobs = sut.read_instances(limit=1)
    assert len(jobs) == 1
    assert jobs[0].job_id == 'j3'


def test_job_id_match(sut):
    sut.store_instances(j(1, 'i1'), j(12, 'i12'), j(11, 'i11'), j(111, 'i111'), j(121, 'i121'))

    assert len(sut.read_instances(parse_criteria('j1'))) == 1
    assert len(sut.read_instances(parse_criteria('j1@'))) == 1
    assert len(sut.read_instances(parse_criteria('j1@i1'))) == 1
    assert len(sut.read_instances(parse_criteria('@i1'))) == 1
    assert len(sut.read_instances(parse_criteria('i1'))) == 1

    assert len(sut.read_instances(parse_criteria('j1', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_instances(parse_criteria('j1@', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_instances(parse_criteria('j1@i1', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_instances(parse_criteria('@i1', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_instances(parse_criteria('i1', MatchingStrategy.PARTIAL))) == 5

    assert len(sut.read_instances(parse_criteria('j1?1', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_instances(parse_criteria('j1?1@', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_instances(parse_criteria('j1?1@i1?1', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_instances(parse_criteria('@i1?1', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_instances(parse_criteria('i1?1', MatchingStrategy.FN_MATCH))) == 2


def test_cleanup(sut):
    sut.store_instances(j(1, sec=-120), j(2), j(3, sec=-240), j(4, sec=-10), j(5, sec=-60))

    sut.clean_up(1, parse_iso8601_duration('PT50S'))
    jobs = sut.read_instances()
    assert len(jobs) == 1
    assert jobs[0].job_id == 'j2'


def test_interval(sut):
    sut.store_instances(j(1, created=dt(2023, 4, 23), completed=dt(2023, 4, 23)))
    sut.store_instances(j(2, created=dt(2023, 4, 22), completed=dt(2023, 4, 22, 23, 59, 59)))
    sut.store_instances(j(3, created=dt(2023, 4, 22), completed=dt(2023, 4, 22, 23, 59, 58)))

    ic = IntervalCriteria(event=LifecycleEvent.ENDED, from_dt=dt(2023, 4, 23))
    jobs = sut.read_instances(InstanceMatchingCriteria(interval_criteria=ic))
    assert jobs.job_ids == ['j1']

    ic = IntervalCriteria(event=LifecycleEvent.ENDED, to_dt=dt(2023, 4, 22, 23, 59, 59))
    jobs = sut.read_instances(InstanceMatchingCriteria(interval_criteria=ic))
    assert sorted(jobs.job_ids) == ['j2', 'j3']

    ic = IntervalCriteria(event=LifecycleEvent.ENDED, to_dt=dt(2023, 4, 22, 23, 59, 59), include_to=False)
    jobs = sut.read_instances(InstanceMatchingCriteria(interval_criteria=ic))
    assert jobs.job_ids == ['j3']

    ic = IntervalCriteria(event=LifecycleEvent.ENDED, from_dt=dt(2023, 4, 22, 23, 59, 59), to_dt=dt(2023, 4, 23))
    jobs = sut.read_instances(InstanceMatchingCriteria(interval_criteria=ic))
    assert sorted(jobs.job_ids) == ['j1', 'j2']


def test_warning(sut):
    sut.store_instances(j(1), j(2, warnings={'w1': 1}), j(3))
    jobs = sut.read_instances(InstanceMatchingCriteria(state_criteria=StateCriteria(warning=True)))

    assert jobs.job_ids == ['j2']
