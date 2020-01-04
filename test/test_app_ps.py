"""
Tests :mod:`app` module
Command: ps
"""
import os
import tempfile
from multiprocessing import Process
from pathlib import Path

from test.util import run_app


def test_ps(capsys):
    tmp_dir = tempfile.mkdtemp()
    fifo1 = Path(tmp_dir, 'fifo1')
    os.mkfifo(fifo1)

    Process(target=run_app, args=('exec cat < ' + str(fifo1),)).start()
    try:
        run_app('ps')
    finally:
        with open(fifo1, 'w') as fifo:
            fifo.write('\n')
        os.remove(fifo1)
        os.rmdir(tmp_dir)

    output = capsys.readouterr().out
    assert 'cat' in output
