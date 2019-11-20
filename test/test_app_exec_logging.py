"""
Tests :mod:`app` module
Command: exec
Description: Test that logging is configured according to CLI options and/or configuration file
"""
import logging

import pytest

from taro import log
from test.util import run_app, create_test_config, remove_test_config


@pytest.fixture(autouse=True)
def remove_config_if_created():
    yield
    remove_test_config()


def test_logging_enabled_by_default():
    create_test_config(dict())
    run_app('exec -C test.yaml echo')
    assert not log.is_disabled()


def test_logging_enabled_in_config():
    create_test_config({"log": {"enabled": True}})
    run_app('exec -C test.yaml echo')
    assert not log.is_disabled()


def test_logging_disabled_in_config():
    create_test_config({"log": {"enabled": False}})
    run_app('exec -C test.yaml echo')
    assert log.is_disabled()


def test_logging_enabled_cli_override():
    create_test_config({"log": {"enabled": False}})
    run_app('exec -C test.yaml --log-enabled true echo')
    assert not log.is_disabled()


def test_logging_disabled():
    run_app('exec --log-enabled false echo')
    assert log.is_disabled()


def test_logging_stdout_level_in_config():
    create_test_config({"log": {"stdout": {"level": "error"}}})
    run_app('exec -C test.yaml echo')
    assert logging.ERROR == log.get_console_level()


def test_logging_stdout_level_cli_override():
    create_test_config({"log": {"stdout": {"level": "error"}}})
    run_app('exec -C test.yaml --log-stdout warn echo')
    assert logging.WARN == log.get_console_level()
