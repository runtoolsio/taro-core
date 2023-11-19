import pytest

from tarotools.taro import client
from tarotools.taro.client import APIClient, APIErrorType, ErrorCode, ReleaseResult, StopResult
from tarotools.taro.jobs.api import APIServer
from tarotools.taro.jobs.criteria import JobRunIdCriterion, JobRunAggregatedCriteria, parse_criteria
from tarotools.taro.run import TerminationStatus, RunState
from tarotools.taro.test.instance import TestJobInstanceBuilder


@pytest.fixture(autouse=True)
def job_instances():
    server = APIServer()

    j1 = TestJobInstanceBuilder('j1', 'i1').add_exec_phase().build()
    j1.last_output = [('Meditate, do not delay, lest you later regret it.', False)]
    server.register_instance(j1)

    j2 = TestJobInstanceBuilder('j2', 'i2').add_approval_phase().build()
    server.register_instance(j2)

    assert server.start()
    try:
        j1.run_new_thread(daemon=True)
        j2.run_new_thread(daemon=True)
        yield j1, j2
    finally:
        server.close()


def test_error_not_found():
    with APIClient() as c:
        _, errors = c.send_request('/no-such-api')
    assert errors[0].error_type == APIErrorType.API_CLIENT
    assert errors[0].response_error.code == ErrorCode.NOT_FOUND


def test_instances_api():
    multi_resp = client.get_active_runs()
    instances = {inst.job_id: inst for inst in multi_resp.responses}
    assert instances['j1'].run.lifecycle.run_state == RunState.EXECUTING
    assert instances['j2'].run.lifecycle.run_state == RunState.PENDING

    multi_resp_j1 = client.get_active_runs(parse_criteria('j1'))
    multi_resp_j2 = client.get_active_runs(parse_criteria('j2'))
    assert multi_resp_j1.responses[0].job_id == 'j1'
    assert multi_resp_j2.responses[0].job_id == 'j2'

    assert not any([multi_resp.errors, multi_resp_j1.errors, multi_resp_j2.errors])


def test_release_waiting_state(client):
    instances, errors = client.release_waiting_instances(TerminationStatus.PENDING,
                                                         JobRunAggregatedCriteria(JobRunIdCriterion('j2', '')))
    assert not errors
    assert instances[0].instance_metadata.id.job_id == 'j2'
    assert instances[0].release_result == ReleaseResult.RELEASED


def test_not_released_waiting_state(client):
    instances, errors = client.release_waiting_instances(TerminationStatus.QUEUED,
                                                         JobRunAggregatedCriteria(JobRunIdCriterion('*', '')))
    assert not errors
    assert not instances


def test_release_pending_group(client, job_instances):
    instances, errors = client.release_pending_instances('p1')
    assert not errors
    assert instances[0].instance_metadata.id.job_id == 'j1'
    assert instances[0].release_result == ReleaseResult.NOT_APPLICABLE
    assert instances[1].instance_metadata.id.job_id == 'j2'
    assert instances[1].release_result == ReleaseResult.RELEASED

    assert not job_instances[0].released
    assert job_instances[1].released


def test_stop(client, job_instances):
    instances, errors = client.stop_instances(JobRunAggregatedCriteria(JobRunIdCriterion('j1', '')))
    assert not errors
    assert len(instances) == 1
    assert instances[0].instance_metadata.id.job_id == 'j1'
    assert instances[0].stop_result == StopResult.STOP_PERFORMED

    assert job_instances[0].stopped
    assert not job_instances[1].stopped


def test_tail(client):
    instances, errors = client.read_tail()
    assert not errors

    assert instances[0].instance_metadata.id.job_id == 'j1'
    assert instances[0].tail == [['Meditate, do not delay, lest you later regret it.', False]]

    assert instances[1].instance_metadata.id.job_id == 'j2'
    assert not instances[1].tail
