"""
Tests :mod:`app` module
Command: stop
"""
from multiprocessing.context import Process

from test.util import run_app


def test_more_jobs_require_all_flag(capsys):
    p1 = Process(target=run_app, args=('exec --id j1 sleep 5',))
    p1.start()
    p2 = Process(target=run_app, args=('exec --id j1 sleep 5',))
    p2.start()

    run_app('stop j1')

    output = capsys.readouterr().out
    assert 'The criteria matches more than one job' in output
    assert p1.is_alive()
    assert p2.is_alive()
