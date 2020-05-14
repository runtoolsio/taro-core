"""
Tests :mod:`app` module
Command: job [disable|list-disabled]
"""
import pytest

from test.util import run_app, create_test_config, test_db_path, remove_test_config, remove_test_db


@pytest.fixture(autouse=True)
def remove_config_if_created():
    yield
    remove_test_config()
    remove_test_db()


def test_disable_jobs(capsys):
    create_test_config({"persistence": {"enabled": True, "type": "sqlite", "database": str(test_db_path())}})
    run_app('job -C test.yaml disable j1 j2')
    output = capsys.readouterr().out
    assert 'j1' in output
    assert 'j2' in output

    run_app('job -C test.yaml list-disabled')
    output = capsys.readouterr().out
    assert 'j1' in output
    assert 'j2' in output
