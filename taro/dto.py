from typing import Dict, Any

from taro import util, JobInstanceID
from taro.jobs.execution import ExecutionError, ExecutionState, ExecutionLifecycle
from taro.jobs.job import JobInfo


def _format_td(td):
    if td is None:
        return None
    return td.isoformat()


def to_info_dto(info) -> Dict[str, Any]:
    lc = info.lifecycle
    state_changes = [{"state": state.name, "changed": _format_td(change)} for state, change in lc.state_changes()]
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
            "state": lc.state().name,
            "created": _format_td(lc.changed(ExecutionState.CREATED)),
            "last_changed": _format_td(lc.last_changed()),
            "execution_started": _format_td(lc.execution_started()),
            "execution_finished": _format_td(lc.execution_finished()),
            "execution_time": lc.execution_time().total_seconds() if lc.execution_started() else None,
        },
        "status": info.status,
        "warnings": info.warnings,
        "exec_error": exec_error
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
                   as_dict['warnings'], exec_error)
