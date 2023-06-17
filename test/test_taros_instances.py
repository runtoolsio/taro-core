from unittest.mock import patch

import bottle
import pytest
from webtest import TestApp

import taros
from taro.client import MultiResponse
from taro.jobs.inst import JobInfoList
from taro.test.execution import lc_running, lc_pending, lc_completed, lc_failed, lc_stopped
from taro.test.job import i


@pytest.fixture
def web_app():
    running_1 = i('running_1', lifecycle=lc_running())
    failed_1 = i('failed_1', lifecycle=lc_failed())
    pending_1 = i('pending_1', lifecycle=lc_pending())
    completed_1 = i('completed_1', lifecycle=lc_completed(term_delta=1)) # Make it completed first
    stopped_1 = i('stopped_1', lifecycle=lc_stopped())

    active_instances = [running_1, pending_1]
    ended_instances = JobInfoList([completed_1, failed_1, stopped_1])

    bottle.debug(True)

    with patch('taro.client.read_jobs_info', return_value=MultiResponse(active_instances, [])):
        with patch('taro.persistence.read_instances', return_value=ended_instances):
            yield TestApp(taros.app.api)

    bottle.debug(False)

def assert_inst(resp, *job_ids):
    assert len(resp.json["_embedded"]["instances"]) == len(job_ids)
    assert [inst["metadata"]["id"]["job_id"] for inst in resp.json["_embedded"]["instances"]] == list(job_ids)

def test_active(web_app):
    resp = web_app.get('/instances')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["instances"]) == 2


def test_incl_finished(web_app):
    resp = web_app.get('/instances?include=finished')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["instances"]) == 3

def test_incl_all(web_app):
    resp = web_app.get('/instances?include=all')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["instances"]) == 5

def test_limit_sort_all(web_app):
    resp = web_app.get('/instances?include=all&limit=2&order=asc')
    assert_inst(resp, 'running_1', 'failed_1')

# def test_limit_sort_finished(web_app):
#     resp = web_app.get('/instances?include=finished&limit=2&sort=ended')
#     assert_inst(resp, 'stopped_1', 'completed_1')
