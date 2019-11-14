"""
Tests :mod:`app` module
Command: config
"""

from taro import paths
from test.util import run_app
from test.util import run_app_expect_error


def test_show_default():
    with open(str(paths.default_config_file_path()), 'r') as file:
        config = file.read()

    output = run_app('config show -dc')

    assert config in output


def test_invalid_sub_command():
    output = run_app_expect_error('config no_such_action', SystemExit)

    assert 'invalid choice' in output
