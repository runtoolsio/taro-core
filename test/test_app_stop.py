"""
Tests :mod:`app` module
Command: stop
"""
from multiprocessing.context import Process

from taro.execution import ExecutionState
from test.util import run_app


def test_more_jobs_require_all_flag(capsys):
    pw = Process(target=run_app, args=('exec wait -c 2 ' + ExecutionState.RUNNING.name,))
    pw.start()
    p1 = Process(target=run_app, args=('exec --id j1 sleep 5',), daemon=True)
    p1.start()
    p2 = Process(target=run_app, args=('exec --id j1 sleep 5',), daemon=True)
    p2.start()

    pw.join()  # Wait for both exec to run
    run_app('stop j1')

    output = capsys.readouterr().out
    assert 'more than one job' in output
    assert p1.is_alive()
    assert p2.is_alive()
