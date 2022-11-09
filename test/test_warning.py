import time
from threading import Thread

import pytest

from taro import warning
from taro.jobs import lock
from taro.jobs.runner import RunnerJobInstance
from taro.test.execution import TestExecution
from taro.test.observer import TestWarnObserver


@pytest.fixture
def execution():
    return TestExecution(wait=True)


@pytest.fixture
def job(execution):
    return RunnerJobInstance('j1', execution, lock.NullStateLocker())


@pytest.fixture
def observer(job):
    observer = TestWarnObserver()
    job.add_warning_observer(observer)
    return observer


def test_exec_time_warning(execution, job, observer):
    warning.exec_time_exceeded(job, 'wid', 0.5)
    run_thread = Thread(target=job.run)
    run_thread.start()

    assert not observer.events
    time.sleep(0.1)
    assert not observer.events
    time.sleep(0.5)

    execution.release()
    run_thread.join(1)
    assert len(observer.events) == 1
