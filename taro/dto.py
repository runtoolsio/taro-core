from taro import util
from taro.execution import ExecutionError, ExecutionState, ExecutionLifecycle
from taro.job import JobInstanceData


def job_instance(inst):
    state_changes = [{"state": state.name, "changed": change.isoformat()} for state, change in
                     inst.lifecycle.state_changes()]
    if inst.exec_error:
        exec_error = {"message": inst.exec_error.message, "state": inst.exec_error.exec_state.name}
    else:
        exec_error = None

    return {"job_id": inst.job_id, "instance_id": inst.instance_id,
            "lifecycle": {"state_changes": state_changes}, "progress": inst.progress, "exec_error": exec_error}


def to_job_instance_data(as_dict):
    state_changes = ((ExecutionState[state_change['state']], util.dt_from_utc_str(state_change['changed']))
                     for state_change in as_dict['lifecycle']['state_changes'])
    lifecycle = ExecutionLifecycle(*state_changes)

    if as_dict['exec_error']:
        exec_error = ExecutionError(as_dict['exec_error']['message'], ExecutionState[as_dict['exec_error']['state']])
    else:
        exec_error = None

    return JobInstanceData(
        as_dict['job_id'],
        as_dict['instance_id'],
        lifecycle,
        as_dict['progress'],
        exec_error)
