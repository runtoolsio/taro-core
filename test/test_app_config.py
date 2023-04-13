"""
Tests :mod:`app` module
Command: config
"""
import pytest

from taro import paths
from taro_test_util import run_app


def test_show_default(capsys):
    with open(str(paths.default_config_file_path()), 'r') as file:
        config_to_be_shown = file.read()

    run_app('setup config print -dc')

    assert config_to_be_shown in capsys.readouterr().out


def test_invalid_sub_command(capsys):
    with pytest.raises(SystemExit):
        run_app('setup config no_such_action')

    assert 'invalid choice' in capsys.readouterr().err
