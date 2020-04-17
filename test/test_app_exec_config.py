"""
Tests :mod:`app` module
Command: exec
Description: Tests related to reading config
"""

import pytest

from test.util import run_app, create_test_config, remove_test_config


@pytest.fixture(autouse=True)
def remove_config_if_created():
    yield
    remove_test_config()


def test_missing_plugins_field():
    create_test_config(dict())
    run_app('exec -C test.yaml echo')
    assert True  # No error


def test_plugins_field_not_array():
    create_test_config({"plugins": "plugin"})
    run_app('exec -C test.yaml echo')
    assert True  # No error
