from taro import plugins


def test_plugin_discovered():
    name2module = plugins.discover_ext_plugins('test_', ['test_plugin'], skip_imported=False)
    assert len(name2module) == 1
    assert name2module['test_plugin'].__name__ == 'test_plugin'


def test_invalid_plugin_ignored():
    """Test that error raised during plugin import is captured"""
    name2module = plugins.discover_ext_plugins('test_', ['test_plugin_invalid'])
    assert len(name2module) == 0
