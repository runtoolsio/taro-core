from typing import Optional

import test.plugins
from tarotools.taro import Plugin
from tarotools.taro import plugins


def test_plugin_discovered():
    plugins.load_plugins(['test_plugin'], package=test.plugins)
    name2module = plugins.load_plugins('test_plugin')
    assert len(name2module) == 1
    assert name2module['test_plugin'].__name__ == 'test_plugin'


def test_invalid_plugin_ignored():
    """Test that error raised during plugin import is captured"""
    name2module = plugins.load_plugins('test_', ['test_plugin_invalid'])
    assert len(name2module) == 0


def test_create_plugins():
    name2plugin = plugins.Plugin.fetch_plugins('test_', ['plugin2', 'test_plugin'])
    assert len(name2plugin) == 2
    assert isinstance(name2plugin['plugin2'], Plugin2)
    assert isinstance(name2plugin['test_plugin'], test_plugin.TestPlugin)


def test_non_existing_plugin_ignored():
    name2plugin = plugins.Plugin.fetch_plugins('none', ['plugin2', 'plugin4'])
    assert len(name2plugin) == 1
    assert isinstance(name2plugin['plugin2'], Plugin2)


def test_create_invalid_plugins():
    Plugin2.error_on_init = BaseException('Must be caught')
    name2plugin = plugins.Plugin.fetch_plugins('none', ['plugin2', 'plugin3'])
    assert len(name2plugin) == 1
    assert isinstance(name2plugin['plugin3'], Plugin3)


class Plugin2(Plugin, plugin_name='plugin2'):

    error_on_init: Optional[BaseException] = None

    def __init__(self):
        error_to_raise = Plugin2.error_on_init
        Plugin2.error_on_init = None
        if error_to_raise:
            raise error_to_raise

    def register_instance(self, job_instance):
        pass

    def unregister_instance(self, job_instance):
        pass


class Plugin3(Plugin, plugin_name='plugin3'):
    def register_instance(self, job_instance):
        pass

    def unregister_instance(self, job_instance):
        pass
