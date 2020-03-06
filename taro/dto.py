from collections import OrderedDict
from datetime import datetime

from taro.execution import ExecutionError, ExecutionState
from taro.job import JobInstanceData


def job_instance(inst):
    state_changes = [{"state": state, "changed": change.isoformat()} for state, change in inst.state_changes.items()]
    if inst.exec_error:
        exec_error = {"message": inst.exec_error.message, "state": inst.exec_error.exec_state.name}
    else:
        exec_error = None

    return {"job_id": inst.job_id, "instance_id": inst.instance_id, "state": inst.state.name,
            "state_changes": state_changes, "progress": inst.progress, "exec_error": exec_error}


def to_job_instance_data(as_dict):
    state_changes = OrderedDict(
        ((state_change['state'], datetime.strptime(state_change['changed'], "%Y-%m-%dT%H:%M:%S.%f%z"))
         for state_change in as_dict['state_changes']))

    if as_dict['exec_error']:
        exec_error = ExecutionError(as_dict['exec_error']['message'], ExecutionState[as_dict['exec_error']['state']])
    else:
        exec_error = None

    return JobInstanceData(
        as_dict['job_id'],
        as_dict['instance_id'],
        ExecutionState[as_dict['state']],
        state_changes,
        as_dict['progress'],
        exec_error)
