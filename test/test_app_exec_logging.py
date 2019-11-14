"""
Tests :mod:`app` module
Command: exec
Group: logging
"""

from test.util import run_app


def test_logging_disabled():
    """
    When logging is disabled not even error log messages are allowed
    """

    standard_output = run_app('exec --log-enabled false ls --no-such-option', capture_stderr=False)
    error_output = run_app('exec --log-enabled false ls --no-such-option', capture_stderr=True)
    assert not standard_output
    assert not error_output
