import time
from threading import Thread

import pytest

import tarotools.taro
from tarotools.taro.jobs import lock, warning
from tarotools.taro.test.execution import TestExecution
from tarotools.taro.test.observer import GenericObserver


@pytest.fixture
def execution():
    return TestExecution(wait=True)


@pytest.fixture
def job(execution):
    return tarotools.taro.job_instance('j1', execution, state_locker=lock.NullStateLocker())


@pytest.fixture
def observer(job):
    observer = GenericObserver()from
    job.add_warning_observer(observer)
    return observer


def test_exec_time_warning(execution, job, observer):
    warning.exec_time_exceeded(job, 'wid', 0.5)
    run_thread = Thread(target=job.run)
    run_thread.start()

    assert observer.updates.empty()
    time.sleep(0.1)
    assert observer.updates.empty()
    time.sleep(0.5)

    execution.release()
    run_thread.join(1)
    assert observer.updates.qsize() == 1
