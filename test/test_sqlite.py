import sqlite3
from datetime import datetime

import pytest

from taro import JobInfo, JobInstanceID, ExecutionLifecycle, ExecutionState, ExecutionError
from taro.jobs.db.sqlite import SQLite
from taro.jobs.track import MutableTrackedTask
from taro.util import utc_now


@pytest.fixture
def sut():
    db_con = sqlite3.connect(':memory:')
    sqlite_ = SQLite(db_con)
    sqlite_.check_tables_exist()
    yield sqlite_
    sqlite_.close()


def test_storage(sut):
    jid = JobInstanceID('j1', 'i1')
    lifecycle = ExecutionLifecycle((ExecutionState.CREATED, utc_now()))
    tracking = MutableTrackedTask('task1')
    error = ExecutionError('e1', ExecutionState.ERROR)
    j1 = JobInfo(jid, lifecycle, tracking, 's1', 'e1', {'w': 1}, error, [('p1', 'v1')], u1='v2')

    sut.store_job(j1)
    jobs = sut.read_jobs()

    j = jobs[0]
    assert j1.id == j.id
    assert j1.tracking == j.tracking
    # assert j1 == j
