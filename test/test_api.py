import pytest

from tarotools.taro import client
from tarotools.taro.client import APIClient, APIErrorType, ErrorCode, ApprovalResult, StopResult
from tarotools.taro.jobs.api import APIServer
from tarotools.taro.jobs.criteria import parse_criteria
from tarotools.taro.run import RunState, StandardPhaseNames, TerminationStatus
from tarotools.taro.test.instance import MockJobInstanceBuilder, TestPhase


@pytest.fixture(autouse=True)
def job_instances():
    server = APIServer()

    j1 = MockJobInstanceBuilder('j1', 'i1').add_phase('EXEC', RunState.EXECUTING).build()
    server.register_instance(j1)
    j1.proceed_next_phase()

    j2 = MockJobInstanceBuilder('j2', 'i2').add_phase(StandardPhaseNames.APPROVAL, RunState.PENDING).build()
    server.register_instance(j2)
    j2.proceed_next_phase()

    assert server.start()
    try:
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


def test_approve_pending_instance(job_instances):
    instances, errors = client.approve_pending_instances(StandardPhaseNames.APPROVAL)

    assert not errors
    assert instances[0].instance_metadata.job_id == 'j1'
    assert instances[0].release_result == ApprovalResult.NOT_APPLICABLE
    assert instances[1].instance_metadata.job_id == 'j2'
    assert instances[1].release_result == ApprovalResult.APPROVED

    _, j2 = job_instances
    assert j2.get_typed_phase(TestPhase, StandardPhaseNames.APPROVAL).approved


def test_stop(job_instances):
    instances, errors = client.stop_instances(parse_criteria('j1'))
    assert not errors
    assert len(instances) == 1
    assert instances[0].instance_metadata.job_id == 'j1'
    assert instances[0].stop_result == StopResult.INITIATED

    j1, j2 = job_instances
    assert j1.job_run_info().run.termination.status == TerminationStatus.STOPPED
    assert not j2.job_run_info().run.termination


def test_tail(job_instances):
    j1, j2 = job_instances
    j1.output.add('EXEC', 'Meditate, do not delay, lest you later regret it.', False)

    instances, errors = client.fetch_output()
    assert not errors

    assert instances[0].instance_metadata.job_id == 'j1'
    assert instances[0].output == [['Meditate, do not delay, lest you later regret it.', False]]

    assert instances[1].instance_metadata.job_id == 'j2'
    assert not instances[1].output
