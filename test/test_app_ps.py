"""
Tests :mod:`app` module
Command: ps
"""
import taroapp.view.instance as view_inst
from taro.jobs.execution import ExecutionState
from taro_test_util import run_app, run_app_as_process_and_wait
from taroapp import printer


def test_job_running(capsys):
    run_app_as_process_and_wait('exec -mc sleep 1', wait_for=ExecutionState.RUNNING, daemon=True)

    run_app('ps')
    output = capsys.readouterr().out

    jobs = printer.parse_table(output, view_inst.DEFAULT_COLUMNS)
    assert 'sleep 1' == jobs[0][view_inst.JOB_ID]
    assert ExecutionState.RUNNING.name.casefold() == jobs[0][view_inst.STATE].casefold()


def test_job_waiting(capsys):
    run_app_as_process_and_wait('exec -mc -p val sleep 1', wait_for=ExecutionState.PENDING, daemon=True)

    run_app('ps')
    output = capsys.readouterr().out

    jobs = printer.parse_table(output, view_inst.DEFAULT_COLUMNS)
    assert 'sleep 1' == jobs[0][view_inst.JOB_ID]
    assert ExecutionState.PENDING.name.casefold() == jobs[0][view_inst.STATE].casefold()


def test_job_status(capsys):
    # Shell to use '&&' to combine commands
    run_app_as_process_and_wait('exec -mc --id p_test echo progress1 && sleep 1',
                                wait_for=ExecutionState.RUNNING, daemon=True, shell=True)

    run_app('ps')
    output = capsys.readouterr().out

    jobs = printer.parse_table(output, view_inst.DEFAULT_COLUMNS)
    assert 'progress1' == jobs[0][view_inst.STATUS]


def test_job_instance_filter_false(capsys):
    run_app_as_process_and_wait('exec -mc --id f_test_no_job1 sleep 1', wait_for=ExecutionState.RUNNING, daemon=True)

    run_app('ps f_test_no_job')
    output = capsys.readouterr().out

    jobs = printer.parse_table(output, view_inst.DEFAULT_COLUMNS)
    assert not jobs


def test_job_instance_filter_true(capsys):
    run_app_as_process_and_wait('exec -mc --id f_test_job1 sleep 1', wait_for=ExecutionState.RUNNING, daemon=True)

    run_app('ps f_test_job*')
    output = capsys.readouterr().out

    jobs = printer.parse_table(output, view_inst.DEFAULT_COLUMNS)
    assert len(jobs) == 1
