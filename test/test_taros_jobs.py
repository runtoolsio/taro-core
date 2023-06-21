from unittest.mock import patch

import bottle
import pytest
from webtest import TestApp

import taros
from taro.client import MultiResponse
from taro.jobs.job import Job
from taro.test.execution import lc_running
from taro.test.job import i


@pytest.fixture
def web_app():
    test_jobs = [
        Job('j1', {'prop': 'value1'}),
        Job('j2', {'prop': 'value2'}),
        Job('j3', {'prop': 'value3'}),
    ]
    with patch('taro.repo.read_jobs', return_value=test_jobs):
        yield TestApp(taros.app.api)

    bottle.debug(True)


def test_no_such_job(web_app):
    assert web_app.get('/jobs/no_such_job', expect_errors=True).status_int == 404


@patch('taro.client.read_jobs_info', return_value=MultiResponse([i('j1', lifecycle=lc_running())], []))
def test_job_def_included_for_instance(_, web_app):
    resp = web_app.get('/instances')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["instances"]) == 1
    assert len(resp.json["_embedded"]["jobs"]) == 1
    assert resp.json["_embedded"]["jobs"][0]["properties"]["prop"] == 'value1'
