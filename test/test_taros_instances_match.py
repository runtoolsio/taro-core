import datetime
from datetime import timedelta
from unittest.mock import patch

import bottle
import pytest
from webtest import TestApp

import taros
from taro.client import MultiResponse
from taro.jobs import persistence
from taro.jobs.job import Job
from taro.test.execution import lc_completed, lc_failed, lc_stopped
from taro.test.job import i
from taro.test.persistence import TestPersistence
from taro.util import MatchingStrategy


@pytest.fixture
def web_app():
    first_created = datetime.datetime(2023, 6, 22, 0, 0)
    failed_1 = i('failed_1', lifecycle=lc_failed(start_date=first_created))
    completed_2 = i('completed_2', 'oldest',
                    lifecycle=lc_completed(start_date=first_created + timedelta(minutes=10), term_delta=5))
    stopped_2 = i('stopped_2', lifecycle=lc_stopped(start_date=first_created + timedelta(minutes=11)))
    completed_1_old = i('completed_1', 'old',
                        lifecycle=lc_completed(start_date=first_created + timedelta(minutes=12), term_delta=3))
    completed_1_new = i('completed_1', 'new',
                        lifecycle=lc_completed(start_date=first_created + timedelta(minutes=13), term_delta=2))
    stopped_1 = i('stopped_1', lifecycle=lc_stopped(start_date=first_created + timedelta(hours=4)))

    bottle.debug(True)

    with TestPersistence():
        persistence.store_instances(completed_1_new, completed_2, completed_1_old, failed_1, stopped_1, stopped_2)
        yield TestApp(taros.app.api)

    bottle.debug(False)


@pytest.fixture
def client_mock():
    with patch('taro.client.read_jobs_info', return_value=MultiResponse([], [])) as client_mock:
        yield client_mock


def assert_inst(resp, *job_ids):
    assert len(resp.json["_embedded"]["instances"]) == len(job_ids)
    assert [inst["metadata"]["id"]["job_id"] for inst in resp.json["_embedded"]["instances"]] == list(job_ids)


def assert_stats(resp, *job_stats):
    assert len(resp.json["_embedded"]["stats"]) == len(job_stats)
    assert [(s["job_id"], s["count"]) for s in resp.json["_embedded"]["stats"]] == list(job_stats)


def test_instance_lookup(web_app, client_mock):
    resp = web_app.get('/instances/completed_2@oldest')
    assert resp.json["metadata"]["id"]["job_id"] == 'completed_2'

    criteria = client_mock.call_args[0][0]
    passed_id_criteria = criteria.id_criteria[0]
    assert passed_id_criteria.job_id == 'completed_2'
    assert passed_id_criteria.instance_id == 'oldest'
    assert passed_id_criteria.match_both_ids
    assert passed_id_criteria.strategy == MatchingStrategy.EXACT


@patch('taro.repo.read_jobs', return_value=[Job('stopped_1', {'p1': 'v1'})])
def test_job_property_filter(_, web_app, client_mock):
    assert_inst(web_app.get('/instances?include=all&job_property=p1:v0'))  # Assert empty

    assert_inst(web_app.get('/instances?include=finished&job_property=p1:v1'), 'stopped_1')

    assert_inst(web_app.get('/instances?include=all&job_property=p1:v1'), 'stopped_1')
    assert client_mock.call_args[0][0].jobs == ['stopped_1']


def test_job_filter(web_app, client_mock):
    assert_inst(web_app.get('/instances?include=finished&job=completed_1'), 'completed_1', 'completed_1')
    assert_inst(web_app.get('/instances?include=all&job=completed_1'), 'completed_1', 'completed_1')
    assert client_mock.call_args_list[-1].args[0].jobs == ['completed_1']


def test_id_filter_job(web_app, client_mock):
    assert_inst(web_app.get('/instances?include=finished&id=stop'), 'stopped_1', 'stopped_2')
    assert_inst(web_app.get('/instances?include=all&id=stop'), 'stopped_1', 'stopped_2')

    id_criteria = client_mock.call_args_list[-1].args[0].id_criteria[0]
    assert id_criteria.job_id == 'stop'
    assert id_criteria.instance_id == 'stop'
    assert id_criteria.strategy == MatchingStrategy.PARTIAL


def test_id_filter_instance(web_app, client_mock):
    assert_inst(web_app.get('/instances?include=finished&id=old'), 'completed_1', 'completed_2')
    assert_inst(web_app.get('/instances?include=all&id=old'), 'completed_1', 'completed_2')

    id_criteria = client_mock.call_args_list[-1].args[0].id_criteria[0]
    assert id_criteria.job_id == 'old'
    assert id_criteria.instance_id == 'old'
    assert id_criteria.strategy == MatchingStrategy.PARTIAL


def test_from_to_criteria(web_app):
    assert_inst(web_app.get('/instances?include=finished&from=2023-06-22T00:11&to=2023-06-22T00:12'), 'completed_1',
                'stopped_2')


def test_invalid_from_criteria(web_app):
    resp = web_app.get('/instances?from=xxx', expect_errors=True)
    assert resp.status_int == 422


def test_success_flag(web_app):
    assert_inst(web_app.get('/instances?include=finished&flag=success'), 'completed_1', 'completed_1', 'completed_2')


def test_two_flags(web_app):
    assert_inst(web_app.get('/instances?include=finished&flag=incomplete&flag=aborted&flag=failure'),
                'stopped_1', 'stopped_2', 'failed_1')


def test_invalid_flag(web_app):
    resp = web_app.get('/instances?flag=xxx', expect_errors=True)
    assert resp.status_int == 422


def test_stats_jobs(web_app):
    resp = web_app.get('/stats/jobs')
    assert_stats(resp, ('completed_1', 2), ('completed_2', 1), ('failed_1', 1), ('stopped_1', 1), ('stopped_2', 1))


def test_stats_jobs_filter_by_job(web_app):
    resp = web_app.get('/stats/jobs?job=completed_1')
    assert_stats(resp, ('completed_1', 2))


def test_stats_jobs_filter_by_interval(web_app):
    resp = web_app.get('/stats/jobs?from=2023-06-22T00:12&to=2023-06-22T00:12')
    assert_stats(resp, ('completed_1', 1))


def test_stats_job(web_app):
    resp = web_app.get('/stats/jobs/completed_1')
    assert resp.json["job_id"] == 'completed_1'
    assert resp.json["count"] == 2


def test_stats_job_filter_by_interval(web_app):
    resp = web_app.get('/stats/jobs/completed_1?from=2023-06-22T00:12&to=2023-06-22T00:12')
    assert resp.json["job_id"] == 'completed_1'
    assert resp.json["count"] == 1
