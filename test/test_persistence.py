from tarotools.taro.jobs import persistence


def test_load_sqlite():
    assert persistence.load_persistence('sqlite')
