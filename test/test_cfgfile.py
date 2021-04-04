"""
Tests :mod:`cfgfile` module
Description: Config related tests
"""
import importlib

import pytest

from taro import paths, cfgfile, cfg
from taro_test_util import create_test_config, remove_test_config


@pytest.fixture(autouse=True)
def remove_config_if_created():
    yield
    remove_test_config()


def test_defaults():
    create_test_config(dict())
    cfgfile.load()
    assert not cfg.log_enabled
    assert cfg.log_stdout_level == 'off'
    assert cfg.log_file_level == 'off'
    assert cfg.log_file_path is None
    assert not cfg.persistence_enabled
    assert cfg.plugins == ()


def test_default_config():
    cfgfile.load(paths.default_config_file_path())
    assert cfg.log_enabled
    assert cfg.log_stdout_level == 'warn'
    assert cfg.log_file_level == 'info'
    assert cfg.log_file_path is None
    assert cfg.persistence_enabled
    assert cfg.plugins == ()


def test_plugins_single_value():
    create_test_config({"plugins": "plugin"})
    cfgfile.load()
    assert cfg.plugins == ("plugin",)


def test_plugins_array():
    create_test_config({"plugins": ["p1", "p2"]})
    cfgfile.load()
    assert cfg.plugins == ("p1", "p2")


def test_not_str_type_raises_error():
    create_test_config({"log": {"stdout": {"level": 3}}})  # Non-str value
    with pytest.raises(TypeError):
        cfgfile.load()
