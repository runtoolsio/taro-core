from typing import Dict, Any

from taro import util
from taro.execution import ExecutionError, ExecutionState, ExecutionLifecycle
from taro.job import JobInfo
from taro.warning import Warn


def to_info_dto(info) -> Dict[str, Any]:
    state_changes = [{"state": state.name, "changed": change.isoformat()} for state, change in
                     info.lifecycle.state_changes()]
    warnings = [{"type": w.id, "params": w.params} for w in info.warnings]
    if info.exec_error:
        exec_error = {"message": info.exec_error.message, "state": info.exec_error.exec_state.name}
    else:
        exec_error = None

    return {"job_id": info.job_id, "instance_id": info.instance_id, "lifecycle": {"state_changes": state_changes},
            "status": info.status, "warnings": warnings, "exec_error": exec_error}


def to_job_info(as_dict) -> JobInfo:
    state_changes = ((ExecutionState[state_change['state']], util.dt_from_utc_str(state_change['changed']))
                     for state_change in as_dict['lifecycle']['state_changes'])
    warnings = (Warn(w['id'], w['params']) for w in as_dict['warnings'])
    lifecycle = ExecutionLifecycle(*state_changes)

    if as_dict['exec_error']:
        exec_error = ExecutionError(as_dict['exec_error']['message'], ExecutionState[as_dict['exec_error']['state']])
    else:
        exec_error = None

    return JobInfo(as_dict['job_id'], as_dict['instance_id'], lifecycle, as_dict['status'], warnings, exec_error)
