import pytest

from taro_test_util import reset_config


@pytest.fixture(autouse=True)
def reset_config_before_each_test():
    reset_config()
