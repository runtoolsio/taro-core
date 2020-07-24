"""
Tests :mod:`app` module
Command: exec
"""

import pytest

from taro import runner
from taro.test.observer import TestJobOutputObserver
from test.util import run_app


@pytest.fixture()
def observer():
    observer = TestJobOutputObserver()
    runner.register_output_observer(observer)
    yield observer
    runner.deregister_output_observer(observer)


def test_output_observer(observer: TestJobOutputObserver):
    run_app('exec -mc echo future sound of london')
    assert observer.last_output() == 'future sound of london'
