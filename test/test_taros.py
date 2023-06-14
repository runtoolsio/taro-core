import pytest
from webtest import TestApp

import taros
from taro.jobs import repo
from taro.jobs.repo import JobRepositoryFile
from test.taro_test_util import create_custom_test_config, remove_custom_test_config


@pytest.fixture
def test_app():
    test_file_jobs = {
        'jobs': [
            {
                'id': 'j1',
                'properties': {'prop1': 'value1'}
            }
        ]
    }
    test_file_jobs_path = create_custom_test_config('jobs.yaml', test_file_jobs)
    repo.add_repo(JobRepositoryFile(test_file_jobs_path))

    yield TestApp(taros.app.api)

    remove_custom_test_config('jobs.yaml')

def test_no_such_job(test_app):
    assert test_app.get('/jobs/no_such_job', expect_errors=True).status_int == 404


def test_empty_jobs(test_app):
    create_custom_test_config('jobs.yaml', {})

    resp = test_app.get('/jobs')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["jobs"]) == 0
