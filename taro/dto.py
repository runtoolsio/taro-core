from datetime import datetime

from taro.execution import ExecutionError, ExecutionState
from taro.job import JobInstanceData


def job_instance(inst):
    state_changes = [{"new_state": state_change[0], "changed": state_change[1].isoformat()}
                     for state_change in inst.state_changes]
    if inst.exec_error:
        exec_error = {"message": inst.exec_error.message, "state": inst.exec_error.exec_state.name}
    else:
        exec_error = None

    return {"job_id": inst.job_id, "instance_id": inst.instance_id, "state": inst.state.name,
            "state_changes": state_changes, "exec_error": exec_error}


def to_job_instance_data(as_dict):
    state_changes = ((state_change['new_state'], datetime.strptime(state_change['changed'], "%Y-%m-%dT%H:%M:%S%z"))
                     for state_change in as_dict['state_changes'])

    if as_dict['exec_error']:
        exec_error = ExecutionError(as_dict['exec_error']['message'], ExecutionState[as_dict['exec_error']['state']])
    else:
        exec_error = None

    return JobInstanceData(
        as_dict['job_id'], as_dict['instance_id'], ExecutionState[as_dict['state']], state_changes, exec_error)
