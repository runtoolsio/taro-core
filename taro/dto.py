from typing import Dict, Any

from taro import util
from taro.jobs.execution import ExecutionError, ExecutionState, ExecutionLifecycle
from taro.jobs.job import JobInfo


def to_info_dto(info) -> Dict[str, Any]:
    state_changes = [{"state": state.name, "changed": change.isoformat()} for state, change in
                     info.lifecycle.state_changes()]
    if info.exec_error:
        exec_error = {"message": info.exec_error.message, "state": info.exec_error.exec_state.name}
    else:
        exec_error = None

    return {"job_id": info.job_id, "instance_id": info.instance_id, "lifecycle": {"state_changes": state_changes},
            "status": info.status, "warnings": info.warnings, "exec_error": exec_error}


def to_job_info(as_dict) -> JobInfo:
    state_changes = ((ExecutionState[state_change['state']], util.dt_from_utc_str(state_change['changed']))
                     for state_change in as_dict['lifecycle']['state_changes'])
    lifecycle = ExecutionLifecycle(*state_changes)

    if as_dict['exec_error']:
        exec_error = ExecutionError(as_dict['exec_error']['message'], ExecutionState[as_dict['exec_error']['state']])
    else:
        exec_error = None

    return JobInfo(as_dict['job_id'], as_dict['instance_id'], lifecycle, as_dict['status'], as_dict['warnings'],
                   exec_error)
