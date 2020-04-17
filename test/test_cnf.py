"""
Tests :mod:`cnf` module
Description: Config related tests
"""

import pytest

from taro import cnf
from taro.cnf import Config
from test.util import create_test_config, remove_test_config


@pytest.fixture(autouse=True)
def remove_config_if_created():
    yield
    remove_test_config()


def test_defaults():
    create_test_config(dict())
    c = Config(_read_config())
    assert c.log_enabled
    assert c.log_stdout_level == 'off'
    assert c.log_file_level == 'off'
    assert c.log_file_path is None
    assert not c.persistence_enabled
    assert c.plugins == ()


def test_plugins_single_value():
    create_test_config({"plugins": "plugin"})
    c = Config(_read_config())
    assert c.plugins == ("plugin",)


def test_plugins_array():
    create_test_config({"plugins": ["p1", "p2"]})
    c = Config(_read_config())
    assert c.plugins == ("p1", "p2")


def _read_config():
    return cnf.read_config('test.yaml')
