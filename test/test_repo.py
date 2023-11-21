"""
Tests :mod:`jobs.repo` module
Description: Jobs file repository tests
"""

import pytest

from tarotools.taro import paths
from tarotools.taro.test.testutil import create_custom_test_config, remove_custom_test_config


@pytest.fixture(autouse=True)
def remove_config_if_created():
    yield
    remove_custom_test_config(paths.JOBS_FILE)


def test_defaults():
    create_custom_test_config(paths.JOBS_FILE, repo.JobRepositoryFile.DEF_FILE_CONTENT)
    example_job = repo.JobRepositoryFile.DEF_FILE_CONTENT['jobs'][0]
    assert repo.read_job(example_job['id']).properties == example_job['properties']
