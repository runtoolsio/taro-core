from unittest.mock import patch

import pytest
from webtest import TestApp

import taros
from taro.client import MultiResponse
from taro.jobs.inst import JobInfoList
from taro.test.execution import lc_running, lc_pending, lc_completed
from taro.test.job import i


@pytest.fixture
def web_app():
    active_1 = i('running_1', lifecycle=lc_running())
    pending_1 = i('pending_1', lifecycle=lc_pending())
    active_instances = [active_1, pending_1]

    completed_1 = i('completed_1', lifecycle=lc_completed())
    ended_instances = JobInfoList([completed_1])

    with patch('taro.client.read_jobs_info', return_value=MultiResponse(active_instances, [])):
        with patch('taro.persistence.read_instances', return_value=ended_instances):
            yield TestApp(taros.app.api)


def test_active_instances(web_app):
    resp = web_app.get('/instances')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["instances"]) == 2


def test_finished_instances(web_app):
    resp = web_app.get('/instances?include=finished')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["instances"]) == 1
