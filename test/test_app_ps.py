"""
Tests :mod:`app` module
Command: ps
"""

from taro.execution import ExecutionState
from test.util import run_app, run_app_as_process_and_wait


def test_job_running(capsys):
    run_app_as_process_and_wait('exec sleep 1', wait_for=ExecutionState.RUNNING, daemon=True)

    run_app('ps')
    output = capsys.readouterr().out

    assert 'sleep 1' in output
    assert ExecutionState.RUNNING.name.casefold() in output.casefold()


def test_job_waiting(capsys):
    run_app_as_process_and_wait('exec -w val sleep 1', wait_for=ExecutionState.WAITING, daemon=True)

    run_app('ps')
    output = capsys.readouterr().out

    assert 'sleep 1' in output
    assert ExecutionState.WAITING.name.casefold() in output.casefold()


def test_job_progress(capsys):
    run_app_as_process_and_wait('exec --id p_test --progress echo progress1 && sleep 1',
                                wait_for=ExecutionState.RUNNING, daemon=True, shell=True)

    run_app('ps')
    output = capsys.readouterr().out

    assert 'progress1' in output
