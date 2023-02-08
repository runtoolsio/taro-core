from taro import util
from taro.jobs.execution import ExecutionError, ExecutionState, ExecutionLifecycle
from taro.jobs.job import JobInfo, JobInstanceID


def datetime_str(td):
    if td is None:
        return None
    return td.isoformat()


def to_jobs_dto(jobs):
    return {"jobs": [job.to_dict() for job in jobs.jobs]}


def to_job_info(as_dict) -> JobInfo:
    state_changes = ((ExecutionState[state_change['state']], util.str_to_datetime(state_change['changed']))
                     for state_change in as_dict['lifecycle']['state_changes'])
    lifecycle = ExecutionLifecycle(*state_changes)

    if as_dict['exec_error']:
        exec_error = ExecutionError(as_dict['exec_error']['message'], ExecutionState[as_dict['exec_error']['state']])
    else:
        exec_error = None

    return JobInfo(
        JobInstanceID(as_dict['id']['job_id'], as_dict['id']['instance_id']),
        lifecycle,
        None,  # TODO
        as_dict['status'],
        as_dict['error_output'],
        as_dict['warnings'],
        exec_error,
        as_dict['parameters'],
        **as_dict['user_params']
    )
