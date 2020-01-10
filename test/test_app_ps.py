"""
Tests :mod:`app` module
Command: ps
"""
from multiprocessing import Process

from test.util import run_app


def test_ps(capsys):
    Process(target=run_app, args=('exec sleep 1',), daemon=True).start()
    run_app('ps')

    output = capsys.readouterr().out
    print(output)
