from tarotools.taro import ExecutionState, client
from tarotools.taro.client import APIClient, APIErrorType, ErrorCode
from tarotools.taro.jobs.api import APIServer, InstancesResource
from tarotools.taro.jobs.inst import InstanceMatchingCriteria, IDMatchingCriteria
from tarotools.taro.test.inst import TestJobInstance


def test_error_not_found():
    server = APIServer([])
    server.start()
    with APIClient() as c:
        try:
            _, errors = c.send_request('/no-such-api')
        finally:
            server.close()

    assert errors[0].error_type == APIErrorType.API_CLIENT
    assert errors[0].response_error.code == ErrorCode.NOT_FOUND


def test_instances():
    server = APIServer([InstancesResource()])
    server.add_job_instance(TestJobInstance('j1', 'i1', ExecutionState.RUNNING))
    server.add_job_instance(TestJobInstance('j2', 'i2', ExecutionState.PENDING))
    assert server.start()

    try:
        multi_resp = client.read_instances()
        multi_resp_j1 = client.read_instances(InstanceMatchingCriteria(IDMatchingCriteria('j1', '')))
        multi_resp_j2 = client.read_instances(InstanceMatchingCriteria(IDMatchingCriteria('j2', '')))
    finally:
        server.close()

    assert not any([multi_resp.errors, multi_resp_j1.errors, multi_resp_j2.errors])

    instances = {inst.job_id: inst for inst in multi_resp.responses}
    assert instances['j1'].lifecycle.state == ExecutionState.RUNNING
    assert instances['j2'].lifecycle.state == ExecutionState.PENDING

    assert multi_resp_j1.responses[0].job_id == 'j1'
    assert multi_resp_j2.responses[0].job_id == 'j2'
