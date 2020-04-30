"""
Tests :mod:`app` module
Command: ps
"""
from taro import ps, app
from taro.execution import ExecutionState
from test.util import run_app, run_app_as_process_and_wait


def test_job_running(capsys):
    run_app_as_process_and_wait('exec sleep 1', wait_for=ExecutionState.RUNNING, daemon=True)

    run_app('ps')
    output = capsys.readouterr().out

    jobs = ps.parse_jobs_table(output, app.DEFAULT_PS_COLUMNS)
    assert 'sleep 1' == jobs[0][ps.JOB_ID]
    assert ExecutionState.RUNNING.name.casefold() == jobs[0][ps.STATE].casefold()


def test_job_waiting(capsys):
    run_app_as_process_and_wait('exec -p val sleep 1', wait_for=ExecutionState.PENDING, daemon=True)

    run_app('ps')
    output = capsys.readouterr().out

    jobs = ps.parse_jobs_table(output, app.DEFAULT_PS_COLUMNS)
    assert 'sleep 1' == jobs[0][ps.JOB_ID]
    assert ExecutionState.PENDING.name.casefold() == jobs[0][ps.STATE].casefold()


def test_job_status(capsys):
    # Shell to use '&&' to combine commands
    run_app_as_process_and_wait('exec --id p_test --read-output echo progress1 && sleep 1',
                                wait_for=ExecutionState.RUNNING, daemon=True, shell=True)

    run_app('ps')
    output = capsys.readouterr().out

    jobs = ps.parse_jobs_table(output, app.DEFAULT_PS_COLUMNS)
    assert 'progress1' == jobs[0][ps.STATUS]
