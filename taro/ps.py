import datetime

from tabulate import tabulate

from taro.execution import ExecutionState


def print_jobs(job_instances, header=True):
    headers = ['JOB ID', 'INSTANCE ID', 'CREATED', 'EXECUTION TIME', 'STATE'] if header else ()
    jobs_as_fields = [_job_to_fields(j) for j in job_instances]
    print(tabulate(jobs_as_fields, headers=headers))


def _job_to_fields(j):
    return \
        (j.job_id,
         j.instance_id,
         j.state_changes[ExecutionState.CREATED].astimezone().replace(tzinfo=None),
         execution_time(j),
         j.state.name)


def execution_time(job_instance):
    state = job_instance.state
    if not state.is_executing():
        return 'N/A'

    utc_now = datetime.datetime.now(datetime.timezone.utc)
    state_change = job_instance.state_changes[state]
    return utc_now - state_change


def print_state_change(job_instance):
    print(f"{job_instance.state.name}: {job_instance.job_id}@{job_instance.instance_id}")