"""
Tests :mod:`app` module
Command: exec
"""

import pytest

from taro import runner
from taro.execution import ExecutionState
from taro.test.observer import TestStateObserver
from test.util import run_app, create_test_config, remove_test_config, remove_test_db, test_db_path


@pytest.fixture(autouse=True)
def observer():
    observer = TestStateObserver()
    runner.register_state_observer(observer)
    yield observer
    runner.deregister_state_observer(observer)
    remove_test_config()
    remove_test_db()


def test_disable_job(observer: TestStateObserver):
    create_test_config({"persistence": {"enabled": True, "type": "sqlite", "database": str(test_db_path())}})
    run_app('disable job_to_disable')
    run_app('exec --id job_to_disable echo')

    assert observer.last_job().job_id == 'job_to_disable'
    assert observer.exec_state(-1) == ExecutionState.DISABLED


def test_disable_jobs(observer: TestStateObserver):
    create_test_config({"persistence": {"enabled": True, "type": "sqlite", "database": str(test_db_path())}})
    run_app('disable job1 job3 j.*')  # 'j.*' not a regular expression here as -regex opt not used

    run_app('exec --id job1 echo')
    run_app('exec --id j2 echo')
    run_app('exec --id job3 echo')

    assert observer.last_state('job1') == ExecutionState.DISABLED
    assert observer.last_state('j2') == ExecutionState.COMPLETED
    assert observer.last_state('job3') == ExecutionState.DISABLED


def test_disable_jobs_by_regex(observer: TestStateObserver):
    create_test_config({"persistence": {"enabled": True, "type": "sqlite", "database": str(test_db_path())}})
    run_app('disable -regex disabled.*')

    run_app('exec --id disable echo')
    run_app('exec --id disabled echo')
    run_app('exec --id disabled1 echo')

    assert observer.last_state('disable') == ExecutionState.COMPLETED
    assert observer.last_state('disabled') == ExecutionState.DISABLED
    assert observer.last_state('disabled1') == ExecutionState.DISABLED
