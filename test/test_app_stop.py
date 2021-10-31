"""
Tests :mod:`app` module
Command: stop
"""
import pytest

from taro.jobs.execution import ExecutionState
from taro_test_util import run_app
from test.taro_test_util import run_app_as_process_and_wait


def test_stop_must_specify_job(capsys):
    with pytest.raises(SystemExit):
        run_app('stop')


def test_stop(capsys):
    p1 = run_app_as_process_and_wait('exec -mc --id to_stop sleep 5', wait_for=ExecutionState.RUNNING, daemon=True)
    p2 = run_app_as_process_and_wait('exec -mc --id to_keep sleep 5', wait_for=ExecutionState.RUNNING, daemon=True)

    run_app('stop to_stop')

    p1.join(timeout=1)  # Timeout (1 sec) must be x times smaller than sleeping interval (5 sec)
    assert not p1.is_alive()
    assert p2.is_alive()


def test_more_jobs_require_all_flag(capsys):
    p1 = run_app_as_process_and_wait('exec -mc --id j1 sleep 5', wait_for=ExecutionState.RUNNING, daemon=True)
    p2 = run_app_as_process_and_wait('exec -mc --id j1 sleep 5', wait_for=ExecutionState.RUNNING, daemon=True)

    run_app('stop j1')

    output = capsys.readouterr().out
    assert 'No action performed' in output
    assert p1.is_alive()
    assert p2.is_alive()
