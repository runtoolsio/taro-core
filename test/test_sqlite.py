import sqlite3
from datetime import datetime as dt
from datetime import timedelta

import pytest

from taro import JobInfo, JobInstanceID, ExecutionLifecycle, ExecutionState, ExecutionError, util
from taro.jobs.db.sqlite import SQLite
from taro.jobs.job import parse_criteria, InstanceMatchingCriteria, IntervalCriteria, LifecycleEvent
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
    jid = JobInstanceID('j1', 'i1')
    lifecycle = ExecutionLifecycle((ExecutionState.CREATED, utc_now()))
    tracking = MutableTrackedTask('task1')
    error = ExecutionError('e1', ExecutionState.ERROR)
    j1 = JobInfo(jid, lifecycle, tracking, 's1', 'e1', {'w': 1}, error, (('p1', 'v1'),), u1='v2')

    sut.store_job(j1)
    jobs = sut.read_jobs()

    assert j1 == jobs[0]


def j(c, *, sec=0, created=utc_now(), completed=utc_now()):
    lifecycle = ExecutionLifecycle(
        (ExecutionState.CREATED, created + timedelta(seconds=sec)),
        (ExecutionState.COMPLETED, completed + timedelta(seconds=sec)))
    return JobInfo(JobInstanceID(f"j{c}", util.unique_timestamp_hex()), lifecycle, None, None, None, None, None, None)


def test_last(sut):
    sut.store_job(j(1), j(2), j(1), j(3), j(2))

    jobs = sut.read_jobs(last=True)
    assert len(jobs) == 3
    assert {job.job_id for job in jobs} == {'j1', 'j2', 'j3'}


def test_sort(sut):
    sut.store_job(j(1), j(2, sec=1), j(3, sec=-1))

    jobs = sut.read_jobs()
    assert jobs.job_ids == ['j3', 'j1', 'j2']

    jobs = sut.read_jobs(asc=False)
    assert jobs.job_ids == ['j2', 'j1', 'j3']


def test_limit(sut):
    sut.store_job(j(1), j(2, sec=1), j(3, sec=-1))

    jobs = sut.read_jobs(limit=1)
    assert len(jobs) == 1
    assert jobs[0].job_id == 'j3'


def test_job_id_match(sut):
    sut.store_job(j(1), j(12), j(11), j(111), j(121))

    assert len(sut.read_jobs(parse_criteria('j1'))) == 1
    assert len(sut.read_jobs(parse_criteria('j1', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_jobs(parse_criteria('j1?1', MatchingStrategy.FN_MATCH))) == 2


def test_cleanup(sut):
    sut.store_job(j(1, sec=-120), j(2), j(3, sec=-240), j(4, sec=-10), j(5, sec=-60))

    sut.clean_up(1, parse_iso8601_duration('PT50S'))
    jobs = sut.read_jobs()
    assert len(jobs) == 1
    assert jobs[0].job_id == 'j2'


def test_interval_left(sut):
    sut.store_job(j(1, created=dt(2023, 4, 23), completed=dt(2023, 4, 23)))
    sut.store_job(j(2, created=dt(2023, 4, 22), completed=dt(2023, 4, 22, 23, 59, 59)))

    ic = IntervalCriteria(event=LifecycleEvent.ENDED, from_dt=dt(2023, 4, 23))
    jobs = sut.read_jobs(InstanceMatchingCriteria(interval_criteria=ic))
    assert len(jobs) == 1
    assert jobs[0].job_id == 'j1'
