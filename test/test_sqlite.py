import sqlite3
from datetime import datetime as dt

import pytest

from tarotools.taro.jobs.criteria import IntervalCriterion, JobRunAggregatedCriteria, \
    parse_criteria
from tarotools.taro.jobs.db.sqlite import SQLite
from tarotools.taro.run import RunState, TerminationStatus
from tarotools.taro.test.instance import ended_run as run
from tarotools.taro.util import parse_iso8601_duration, MatchingStrategy


@pytest.fixture
def sut():
    db_con = sqlite3.connect(':memory:')
    sqlite_ = SQLite(db_con)
    sqlite_.check_tables_exist()
    yield sqlite_
    sqlite_.close()


def test_store_and_fetch(sut):
    test_run = run('j1', term_status=TerminationStatus.FAILED)
    sut.store_job_runs(test_run)
    jobs = sut.read_job_runs()

    assert test_run == jobs[0]


def test_last(sut):
    sut.store_job_runs(
        run('j1', 'r1-1', offset_min=1),
        run('j2', 'r2-1', offset_min=2),
        run('j1', 'r1-2', offset_min=3),
        run('j3', 'r3-1', offset_min=4),
        run('j2', 'r2-2', offset_min=5))

    jobs = sut.read_job_runs(last=True)
    assert len(jobs) == 3
    assert [job.run_id for job in jobs] == ['r1-2', 'r3-1', 'r2-2']


def test_sort(sut):
    sut.store_job_runs(run('j1'), run('j2', offset_min=1), run('j3', offset_min=-1))

    jobs = sut.read_job_runs()
    assert jobs.job_ids == ['j3', 'j1', 'j2']

    jobs = sut.read_job_runs(asc=False)
    assert jobs.job_ids == ['j2', 'j1', 'j3']


def test_limit(sut):
    sut.store_job_runs(run('1'), run('2', offset_min=1), run('3', offset_min=-1))

    jobs = sut.read_job_runs(limit=1)
    assert len(jobs) == 1
    assert jobs[0].job_id == '3'


def test_offset(sut):
    sut.store_job_runs(run('1'), run('2', offset_min=1), run('3', offset_min=-1))

    jobs = sut.read_job_runs(offset=2)
    assert len(jobs) == 1
    assert jobs[0].job_id == '2'


def test_job_id_match(sut):
    sut.store_job_runs(run('j1', 'i1'), run('j12', 'i12'), run('j11', 'i11'), run('j111', 'i111'), run('j121', 'i121'))

    assert len(sut.read_job_runs(parse_criteria('j1'))) == 1
    assert len(sut.read_job_runs(parse_criteria('j1@'))) == 1
    assert len(sut.read_job_runs(parse_criteria('j1@i1'))) == 1
    assert len(sut.read_job_runs(parse_criteria('@i1'))) == 1
    assert len(sut.read_job_runs(parse_criteria('i1'))) == 1

    assert len(sut.read_job_runs(parse_criteria('j1', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_job_runs(parse_criteria('j1@', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_job_runs(parse_criteria('j1@i1', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_job_runs(parse_criteria('@i1', MatchingStrategy.PARTIAL))) == 5
    assert len(sut.read_job_runs(parse_criteria('i1', MatchingStrategy.PARTIAL))) == 5

    assert len(sut.read_job_runs(parse_criteria('j1?1', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_job_runs(parse_criteria('j1?1@', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_job_runs(parse_criteria('j1?1@i1?1', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_job_runs(parse_criteria('@i1?1', MatchingStrategy.FN_MATCH))) == 2
    assert len(sut.read_job_runs(parse_criteria('i1?1', MatchingStrategy.FN_MATCH))) == 2


def test_cleanup(sut):
    sut.store_job_runs(run('j1', offset_min=-120), run('j2'), run('j3', offset_min=-240), run('j4', offset_min=-10), run('j5', offset_min=-60))

    sut.clean_up(1, parse_iso8601_duration('PT50S'))
    jobs = sut.read_job_runs()
    assert len(jobs) == 1
    assert jobs[0].job_id == 'j2'


def test_interval(sut):
    sut.store_job_runs(run('j1', created=dt(2023, 4, 23), completed=dt(2023, 4, 23)))
    sut.store_job_runs(run('j2', created=dt(2023, 4, 22), completed=dt(2023, 4, 22, 23, 59, 59)))
    sut.store_job_runs(run('j3', created=dt(2023, 4, 22), completed=dt(2023, 4, 22, 23, 59, 58)))

    ic = IntervalCriterion(run_state=RunState.ENDED, from_dt=dt(2023, 4, 23))
    jobs = sut.read_job_runs(JobRunAggregatedCriteria(interval_criteria=ic))
    assert jobs.job_ids == ['j1']

    ic = IntervalCriterion(run_state=RunState.ENDED, to_dt=dt(2023, 4, 22, 23, 59, 59))
    jobs = sut.read_job_runs(JobRunAggregatedCriteria(interval_criteria=ic))
    assert sorted(jobs.job_ids) == ['j2', 'j3']

    ic = IntervalCriterion(run_state=RunState.ENDED, to_dt=dt(2023, 4, 22, 23, 59, 59), include_to=False)
    jobs = sut.read_job_runs(JobRunAggregatedCriteria(interval_criteria=ic))
    assert jobs.job_ids == ['j3']

    ic = IntervalCriterion(run_state=RunState.ENDED, from_dt=dt(2023, 4, 22, 23, 59, 59), to_dt=dt(2023, 4, 23))
    jobs = sut.read_job_runs(JobRunAggregatedCriteria(interval_criteria=ic))
    assert sorted(jobs.job_ids) == ['j1', 'j2']
