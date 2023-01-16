from typing import Dict, Any

from taro import util
from taro.jobs.execution import ExecutionError, ExecutionState, ExecutionLifecycle
from taro.jobs.job import JobInfo, JobInstanceID


def datetime_str(td):
    if td is None:
        return None
    return td.isoformat()


def to_info_dto(info) -> Dict[str, Any]:
    lc = info.lifecycle
    state_changes = [{"state": state.name, "changed": datetime_str(change)} for state, change in lc.state_changes]
    if info.exec_error:
        exec_error = {"message": info.exec_error.message, "state": info.exec_error.exec_state.name}
    else:
        exec_error = None

    return {
        "id": {
            "job_id": info.job_id,
            "instance_id": info.instance_id,
        },
        "lifecycle": {
            "state_changes": state_changes,
            "state": lc.state.name,
            "created": datetime_str(lc.changed(ExecutionState.CREATED)),
            "last_changed": datetime_str(lc.last_changed),
            "execution_started": datetime_str(lc.execution_started),
            "execution_finished": datetime_str(lc.execution_finished),
            "execution_time": lc.execution_time.total_seconds() if lc.execution_started else None,
        },
        "status": info.status,
        "warnings": info.warnings,
        "exec_error": exec_error,
        "parameters": info.parameters,
        "user_params": info.user_params
    }


def to_jobs_dto(jobs):
    return {"jobs": [to_info_dto(job) for job in jobs.jobs]}


def to_job_info(as_dict) -> JobInfo:
    state_changes = ((ExecutionState[state_change['state']], util.dt_from_utc_str(state_change['changed']))
                     for state_change in as_dict['lifecycle']['state_changes'])
    lifecycle = ExecutionLifecycle(*state_changes)

    if as_dict['exec_error']:
        exec_error = ExecutionError(as_dict['exec_error']['message'], ExecutionState[as_dict['exec_error']['state']])
    else:
        exec_error = None

    return JobInfo(JobInstanceID(as_dict['id']['job_id'], as_dict['id']['instance_id']), lifecycle, as_dict['status'],
                   as_dict['warnings'], exec_error, as_dict['parameters'], **as_dict['user_params'])
