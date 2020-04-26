import abc
from typing import Optional

from taro import plugins, PluginBase, JobControl


def test_plugin_discovered():
    name2module = plugins.discover_ext_plugins('test_', ['test_plugin'], skip_imported=False)
    assert len(name2module) == 1
    assert name2module['test_plugin'].__name__ == 'test_plugin'


def test_invalid_plugin_ignored():
    """Test that error raised during plugin import is captured"""
    name2module = plugins.discover_ext_plugins('test_', ['test_plugin_invalid'])
    assert len(name2module) == 0


class Plugin2(PluginBase, plugin_name='plugin2'):
    error_on_init: Optional[BaseException] = None

    def __init__(self):
        error_to_raise = Plugin2.error_on_init
        Plugin2.error_on_init = None
        if error_to_raise:
            raise error_to_raise

    def new_job_instance(self, job_instance: JobControl):
        pass


class Plugin3(PluginBase, plugin_name='plugin3'):
    def new_job_instance(self, job_instance: JobControl):
        pass
