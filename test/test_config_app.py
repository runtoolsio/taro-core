"""
Tests :mod:`app` module
Command: config
"""
import contextlib
import io

import pytest

from taro import app
from taro import paths


def test_show_default():
    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        app.main(['config', 'show', '-dc'])
    output = stdout.getvalue()

    with open(str(paths.default_config_file_path()), 'r') as file:
        config = file.read()

    assert config in output


def test_invalid_sub_command():
    stderr = io.StringIO()
    with contextlib.redirect_stderr(stderr):
        with pytest.raises(SystemExit):
            app.main(['config', 'no_such_action'])
    output = stderr.getvalue()

    assert 'invalid choice' in output
