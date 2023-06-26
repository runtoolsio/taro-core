"""
Tests for taros which do not require any other fixture than the app.
"""

from unittest.mock import patch

import pytest
from webtest import TestApp

import taros
from taro.client import MultiResponse
from taro.jobs.inst import JobInstances
from taro.jobs.persistence import PersistenceDisabledError


@pytest.fixture
def web_app():
    yield TestApp(taros.app.api)


@patch('taro.repo.read_jobs', return_value=[])
def test_empty_jobs(_, web_app):
    resp = web_app.get('/jobs')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["jobs"]) == 0


@patch('taro.client.read_job_instances', return_value=MultiResponse([], []))
def test_empty_instances(_, web_app):
    resp = web_app.get('/instances')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["instances"]) == 0


@patch('taro.persistence.read_instances', return_value=JobInstances([]))
def test_empty_finished_instances(_, web_app):
    resp = web_app.get('/instances?include=finished')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["instances"]) == 0


@patch('taro.persistence.read_instances', side_effect=PersistenceDisabledError)
def test_instances_conflict_persistence_disabled(_, web_app):
    resp = web_app.get('/instances?include=finished', expect_errors=True)
    assert resp.status_int == 409
