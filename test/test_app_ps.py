"""
Tests :mod:`app` module
Command: ps
"""
from concurrent.futures.thread import ThreadPoolExecutor

from test.util import run_app


def test_ps(capsys):
    executor = ThreadPoolExecutor()
    try:
        executor.submit(lambda: run_app('exec sleep 1'))
        run_app('ps')
    finally:
        executor.shutdown()

    output = capsys.readouterr().out
    assert 'sleep 1' in output
    assert 'running' in output
