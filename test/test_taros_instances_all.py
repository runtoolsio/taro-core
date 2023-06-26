from unittest.mock import patch

import bottle
import pytest
from webtest import TestApp

import taros
from taro.client import MultiResponse
from taro.jobs import persistence
from taro.test.execution import lc_running, lc_pending, lc_completed, lc_failed, lc_stopped, lc_queued
from taro.test.job import i
from taro.test.persistence import TestPersistence


@pytest.fixture
def web_app():
    stopped_1 = i('stopped_1', lifecycle=lc_stopped())
    queued_1 = i('queued_1', lifecycle=lc_queued())
    completed_1_new = i('completed_1', 'new', lifecycle=lc_completed())
    failed_1 = i('failed_1', lifecycle=lc_failed(term_delta=10))  # Make with the newest ended time
    running_1 = i('running_1', lifecycle=lc_running())
    completed_1_old = i('completed_1', 'old', lifecycle=lc_completed())  # Make it the second newest ended one
    completed_2 = i('completed_2', lifecycle=lc_completed())  # Make it the newest ended one
    pending_1 = i('pending_1', lifecycle=lc_pending())  # Make it the newest one

    active_instances = [running_1, queued_1, pending_1]

    bottle.debug(True)

    with TestPersistence():
        persistence.store_instances(completed_1_new, completed_2, completed_1_old, failed_1, stopped_1)
        with patch('taro.client.read_job_instances', return_value=MultiResponse(active_instances, [])):
            yield TestApp(taros.app.api)

    bottle.debug(False)


def assert_inst(resp, *job_ids):
    assert len(resp.json["_embedded"]["instances"]) == len(job_ids)
    assert [inst["metadata"]["id"]["job_id"] for inst in resp.json["_embedded"]["instances"]] == list(job_ids)


def test_default_active(web_app):
    resp = web_app.get('/instances')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["instances"]) == 3


def test_incl_active(web_app):
    resp = web_app.get('/instances?include=active')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["instances"]) == 3


def test_incl_finished(web_app):
    resp = web_app.get('/instances?include=finished')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["instances"]) == 5


def test_incl_all(web_app):
    resp = web_app.get('/instances?include=all')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["instances"]) == 8


def test_sort_asc(web_app):
    resp = web_app.get('/instances?include=all&order=asc')
    assert_inst(resp, 'stopped_1', 'queued_1', 'completed_1', 'failed_1', 'running_1', 'completed_1', 'completed_2',
                'pending_1')


def test_sort_default_desc(web_app):
    resp = web_app.get('/instances?include=all')
    assert_inst(resp, 'pending_1', 'completed_2', 'completed_1', 'running_1', 'failed_1', 'completed_1', 'queued_1',
                'stopped_1')


def test_limit_sort_asc(web_app):
    resp = web_app.get('/instances?include=all&limit=2&order=asc')
    assert_inst(resp, 'stopped_1', 'queued_1')


def test_limit_sort_desc(web_app):
    resp = web_app.get('/instances?include=all&limit=2&order=desc')
    assert_inst(resp, 'pending_1', 'completed_2')


def test_limit_sort_finished(web_app):
    resp = web_app.get('/instances?include=finished&limit=2&sort=ended&order=desc')
    assert_inst(resp, 'failed_1', 'completed_2')


def test_offset_sort_finished(web_app):
    resp = web_app.get('/instances?include=finished&offset=3&sort=ended&order=desc')
    assert_inst(resp, 'completed_1', 'stopped_1')


def test_limit_offset_active(web_app):
    resp = web_app.get('/instances?include=active&limit=1&offset=1')
    assert_inst(resp, 'running_1')
