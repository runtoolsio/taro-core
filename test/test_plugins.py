from typing import Optional

import test.plugins
from plugins import test_plugin
from tarotools.taro import Plugin
from tarotools.taro import plugins


def test_plugin_discovered():
    name2module = plugins.load_modules(['test_plugin'], package=test.plugins)
    assert len(name2module) == 1
    assert name2module['test_plugin'].__name__ == 'test.plugins.test_plugin'


def test_invalid_plugin_ignored():
    """Test that error raised during plugin import is captured"""
    name2module = plugins.load_modules(['test_plugin_invalid'], package=test.plugins)
    assert len(name2module) == 0


def test_fetch_plugins():
    plugins.load_modules(['test_plugin'], package=test.plugins)
    name2plugin = plugins.Plugin.fetch_plugins(['plugin2', 'test_plugin'])
    assert len(name2plugin) == 2
    assert isinstance(name2plugin['plugin2'], Plugin2)
    assert isinstance(name2plugin['test_plugin'], test.plugins.test_plugin.TestPlugin)


def test_fetch_plugin_twice():
    name2plugin_first = plugins.Plugin.fetch_plugins(['plugin3'])
    name2plugin_second = plugins.Plugin.fetch_plugins(['plugin3'])

    assert name2plugin_first != name2plugin_second


def test_fetch_plugin_cached():
    name2plugin_first = plugins.Plugin.fetch_plugins(['plugin3'], cached=True)
    name2plugin_second = plugins.Plugin.fetch_plugins(['plugin3'], cached=True)

    assert name2plugin_first == name2plugin_second


def test_non_existing_plugin_ignored():
    name2plugin = plugins.Plugin.fetch_plugins(['plugin2', 'plugin4'])
    assert len(name2plugin) == 1
    assert isinstance(name2plugin['plugin2'], Plugin2)


def test_create_invalid_plugins():
    Plugin2.error_on_init = Exception('Must be caught')
    name2plugin = plugins.Plugin.fetch_plugins(['plugin2', 'plugin3'])
    assert len(name2plugin) == 1
    assert isinstance(name2plugin['plugin3'], Plugin3)


class Plugin2(Plugin, plugin_name='plugin2'):
    error_on_init: Optional[Exception] = None

    def __init__(self):
        error_to_raise = Plugin2.error_on_init
        Plugin2.error_on_init = None
        if error_to_raise:
            raise error_to_raise

    def register_instance(self, job_instance):
        pass

    def unregister_instance(self, job_instance):
        pass

    def close(self):
        pass


class Plugin3(Plugin, plugin_name='plugin3'):

    def register_instance(self, job_instance):
        pass

    def unregister_instance(self, job_instance):
        pass

    def close(self):
        pass
