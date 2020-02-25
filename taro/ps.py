import datetime

from tabulate import tabulate

from taro.execution import ExecutionState


def print_jobs(job_instances):
    headers = ['JOB ID', 'INSTANCE ID', 'STATE', 'CREATED', 'EXECUTION TIME']

    jobs_as_fields = [
        (j.job_id,
         j.instance_id,
         j.state.name,
         j.state_changes[ExecutionState.CREATED].astimezone().replace(tzinfo=None),
         execution_time(j)) for j in job_instances]
    print(tabulate(jobs_as_fields, headers=headers))


def execution_time(job_instance):
    state = job_instance.state
    if not state.is_executing():
        return 'N/A'

    utc_now = datetime.datetime.now(datetime.timezone.utc)
    state_change = job_instance.state_changes[state]
    return utc_now - state_change
