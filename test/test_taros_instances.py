from unittest.mock import patch

import bottle
import pytest
from webtest import TestApp

import taros
from taro.client import MultiResponse
from taro.jobs import persistence
from taro.jobs.job import Job
from taro.test.execution import lc_running, lc_pending, lc_completed, lc_failed, lc_stopped
from taro.test.job import i
from taro.test.persistence import TestPersistence


@pytest.fixture
def web_app():
    running_1 = i('running_1', lifecycle=lc_running())
    failed_1 = i('failed_1', lifecycle=lc_failed())
    pending_1 = i('pending_1', lifecycle=lc_pending(delta=300))  # Make it the oldest one
    completed_1_new = i('completed_1', 'new',
                        lifecycle=lc_completed(term_delta=1))  # Make it the third oldest ended one
    completed_1_old = i('completed_1', 'old', lifecycle=lc_completed(delta=100))  # Make it the second oldest ended one
    completed_2 = i('completed_2', lifecycle=lc_completed(delta=200))  # Make it the oldest ended one
    stopped_1 = i('stopped_1', lifecycle=lc_stopped())

    active_instances = [running_1, pending_1]

    bottle.debug(True)

    with TestPersistence():
        persistence.store_instances(completed_1_new, completed_1_old, completed_2, failed_1, stopped_1)
        with patch('taro.client.read_jobs_info', return_value=MultiResponse(active_instances, [])):
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
    assert len(resp.json["_embedded"]["instances"]) == 5


def test_incl_all(web_app):
    resp = web_app.get('/instances?include=all')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["instances"]) == 7


def test_sort_default(web_app):
    resp = web_app.get('/instances?include=all')
    assert_inst(resp, 'stopped_1', 'completed_1', 'failed_1', 'running_1', 'completed_1', 'completed_2', 'pending_1')


def test_sort_asc(web_app):
    resp = web_app.get('/instances?include=all&order=asc')
    assert_inst(resp, 'pending_1', 'completed_2', 'completed_1', 'running_1', 'failed_1', 'completed_1', 'stopped_1')


def test_limit_sort_all_in_period(web_app):
    resp = web_app.get('/instances?include=all&limit=2&order=asc')
    assert_inst(resp, 'pending_1', 'completed_2')


def test_limit_sort_finished(web_app):
    resp = web_app.get('/instances?include=finished&limit=2&sort=ended&order=asc')
    assert_inst(resp, 'completed_2', 'completed_1')


@patch('taro.repo.read_jobs', return_value=[Job('stopped_1', {'p1': 'v1'})])
def test_job_property_filter(_, web_app):
    assert_inst(web_app.get('/instances?include=finished&job_property=p1:v0'))  # Assert empty
    assert_inst(web_app.get('/instances?include=finished&job_property=p1:v1'), 'stopped_1')
