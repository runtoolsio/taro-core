import pytest
from webtest import TestApp

import taros


@pytest.fixture
def test_app():
    return TestApp(taros.app.api)

def test_no_such_job(test_app):
    assert test_app.get('/jobs/j1', expect_errors=True).status_int == 404


def test_empty_jobs(test_app):
    resp = test_app.get('/jobs')
    assert resp.status_int == 200
    assert len(resp.json["_embedded"]["jobs"]) == 0
