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

    command = 'exec --log-enabled false ls --no-such-option'
    standard_output = run_app(command, capture_stderr=False)
    error_output = run_app(command, capture_stderr=True)
    assert not standard_output
    assert not error_output
