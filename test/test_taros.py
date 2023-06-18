from unittest.mock import patch

import pytest
from webtest import TestApp

import taros
from taro.client import MultiResponse
from taro.jobs import repo
from taro.jobs.inst import JobInfoList
from taro.jobs.job import Job
from taro.jobs.persistence import PersistenceDisabledError
from taro.jobs.repo import JobRepositoryFile
from taro.test.execution import lc_running
from taro.test.job import i
from test.taro_test_util import create_custom_test_config


@pytest.fixture
def web_app():
    test_jobs = [
        Job('j1', {'prop': 'value1'}),
        Job('j2', {'prop': 'value2'}),
        Job('j3', {'prop': 'value3'}),
    ]
    with patch('taro.repo.read_jobs', return_value=test_jobs):
        yield TestApp(taros.app.api)


def test_no_such_job(web_app):
    assert web_app.get('/jobs/no_such_job', expect_errors=True).status_int == 404


@patch('taro.repo.read_jobs', return_value=[])
def test_empty_jobs(_, web_app):
    test_file_jobs_path = create_custom_test_config('jobs.yaml', {})
    repo.add_repo(JobRepositoryFile(test_file_jobs_path))

    resp = web_app.get('/jobs')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["jobs"]) == 0


@patch('taro.client.read_jobs_info', return_value=MultiResponse([], []))
def test_empty_instances(_, web_app):
    resp = web_app.get('/instances')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["instances"]) == 0


@patch('taro.persistence.read_instances', return_value=JobInfoList([]))
def test_empty_finished_instances(_, web_app):
    resp = web_app.get('/instances?include=finished')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["instances"]) == 0


@patch('taro.persistence.read_instances', side_effect=PersistenceDisabledError)
def test_instances_conflict_persistence_disabled(_, web_app):
    resp = web_app.get('/instances?include=finished', expect_errors=True)
    assert resp.status_int == 409


@patch('taro.client.read_jobs_info', return_value=MultiResponse([i('j1', lifecycle=lc_running())], []))
def test_job_def_included_for_instance(_, web_app):
    resp = web_app.get('/instances')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["instances"]) == 1
    assert len(resp.json["_embedded"]["jobs"]) == 1
    assert resp.json["_embedded"]["jobs"][0]["properties"]["prop"] == 'value1'
