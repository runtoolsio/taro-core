"""
Tests :mod:`cnf` module
Description: Config related tests
"""

import pytest

from taro import cnf, paths
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


def test_default_config():
    c = Config(cnf.read_config(paths.default_config_file_path()))
    assert c.log_enabled
    assert c.log_stdout_level == 'warn'
    assert c.log_file_level == 'info'
    assert c.log_file_path is None
    assert c.persistence_enabled
    assert c.plugins == ()


def test_minimal_config():
    c = Config(cnf.read_config(paths.minimal_config_file_path()))
    assert not c.log_enabled
    assert c.log_stdout_level == 'off'
    assert c.log_file_level == 'off'
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


def test_not_str_type_ignored():
    """When non-str value is used in str field then it gets default value"""
    create_test_config({"log": {"stdout": {"level": 3}}})  # Non-str value
    c = Config(_read_config())
    assert c.log_stdout_level == 'off'


def _read_config():
    return cnf.read_config(paths.lookup_config_file())
