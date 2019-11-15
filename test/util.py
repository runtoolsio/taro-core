import contextlib
import io
from pathlib import Path

import pytest
import yaml

from taro import app


def run_app(command, capture_stderr=False):
    """
    Run command and return recorded stdout or stderr
    :param capture_stderr: return stderr instead of stdout
    :param command: command to run
    :return: output of the executed command
    """
    output = io.StringIO()
    if capture_stderr:
        with contextlib.redirect_stderr(output):
            app.main(command.split())
    else:
        with contextlib.redirect_stdout(output):
            app.main(command.split())
    return output.getvalue()


def run_app_expect_error(command, exception):
    """
    Run command expecting to raise an exception
    :param exception: expected exception
    :param command: command to fail
    :return: stderr of the executed command
    """
    stderr = io.StringIO()
    with contextlib.redirect_stderr(stderr):
        with pytest.raises(exception):
            app.main(command.split())
    return stderr.getvalue()


def create_test_config(config):
    with open(_test_config_path(), 'w') as outfile:
        yaml.dump(config, outfile, default_flow_style=False)


def remove_test_config():
    config = _test_config_path()
    if config.exists():
        config.unlink()


def _test_config_path() -> Path:
    base_path = Path(__file__).parent
    return base_path / 'test.yaml'
