import contextlib
import io

import pytest

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
        with io.StringIO() as buf, contextlib.redirect_stderr(buf):
            app.main(command.split())
            return buf.getvalue()
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
