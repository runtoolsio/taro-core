"""
Tests :mod:`app` module
Command: [disable|list-disabled]
"""
import pytest

import taroapp.view.disabled as view_dis
from taro import cfg
from taroapp import ps
from taro_test_util import run_app, test_db_path, remove_test_db


@pytest.fixture(autouse=True)
def remove_config_if_created():
    cfg.persistence_enabled = True
    cfg.persistence_type = 'sqlite'
    cfg.persistence_database = str(test_db_path())
    yield
    remove_test_db()


def test_disable_jobs(capsys):
    run_app('disable j1 j2')
    run_app('disable -regex j3')
    output = capsys.readouterr().out
    assert 'j1' in output
    assert 'j2' in output
    assert 'j3' in output

    run_app('list-disabled')
    output = capsys.readouterr().out
    disabled = ps.parse_table(output, view_dis.DEFAULT_COLUMNS)

    assert 'j1' in disabled[0][view_dis.JOB_ID]
    assert 'no' in disabled[0][view_dis.REGEX]

    assert 'j2' in disabled[1][view_dis.JOB_ID]
    assert 'no' in disabled[1][view_dis.REGEX]

    assert 'j3' in disabled[2][view_dis.JOB_ID]
    assert 'yes' in disabled[2][view_dis.REGEX]
