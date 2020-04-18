# For plugin testing in test_plugin.py
from taro.test.observer import TestObserver

LISTENER = TestObserver()


def create_listener():
    return LISTENER
