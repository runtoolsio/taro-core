import pytest

from tarotools.taro import ExecutionState
from tarotools.taro.client import APIClient, APIErrorType, ErrorCode, ReleaseResult, StopResult
from tarotools.taro.jobs.api import APIServer
from tarotools.taro.jobs.inst import InstanceMatchingCriteria, IDMatchingCriteria
from tarotools.taro.test.inst import TestJobInstance


@pytest.fixture(autouse=True)
def job_instances():
    server = APIServer()
    j1 = TestJobInstance('j1', 'i1', ExecutionState.RUNNING)
    j1.last_output = [('Meditate, do not delay, lest you later regret it.', False)]
    server.add_job_instance(j1)
    j2 = TestJobInstance('j2', 'i2', ExecutionState.PENDING)
    j2.metadata.pending_group = 'p1'
    server.add_job_instance(j2)
    assert server.start()
    try:
        yield j1, j2
    finally:
        server.close()


@pytest.fixture
def client():
    with APIClient() as c:
        yield c


def test_error_not_found(client):
    _, errors = client.send_request('/no-such-api')
    assert errors[0].error_type == APIErrorType.API_CLIENT
    assert errors[0].response_error.code == ErrorCode.NOT_FOUND


def test_instances_api(client):
    multi_resp = client.read_instances()
    instances = {inst.job_id: inst for inst in multi_resp.responses}
    assert instances['j1'].lifecycle.state == ExecutionState.RUNNING
    assert instances['j2'].lifecycle.state == ExecutionState.PENDING

    multi_resp_j1 = client.read_instances(InstanceMatchingCriteria(IDMatchingCriteria('j1', '')))
    multi_resp_j2 = client.read_instances(InstanceMatchingCriteria(IDMatchingCriteria('j2', '')))
    assert multi_resp_j1.responses[0].job_id == 'j1'
    assert multi_resp_j2.responses[0].job_id == 'j2'

    assert not any([multi_resp.errors, multi_resp_j1.errors, multi_resp_j2.errors])


def test_release_waiting_state(client):
    instances, errors = client.release_waiting_instances(InstanceMatchingCriteria(IDMatchingCriteria('j2', '')),
                                                         ExecutionState.PENDING)
    assert not errors
    assert instances[0].instance_metadata.id.job_id == 'j2'
    assert instances[0].release_result == ReleaseResult.RELEASED


def test_not_released_waiting_state(client):
    instances, errors = client.release_waiting_instances(InstanceMatchingCriteria(IDMatchingCriteria('*', '')),
                                                         ExecutionState.QUEUED)
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
    instances, errors = client.stop_instances(InstanceMatchingCriteria(IDMatchingCriteria('j1', '')))
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
