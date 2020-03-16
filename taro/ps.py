import datetime
from typing import Iterable

from tabulate import tabulate

from taro.execution import ExecutionState


class Column:
    def __init__(self, name, value_fnc):
        self.name = name
        self.value_fnc = value_fnc


JOB_ID = Column('JOB ID', lambda j: j.job_id)
INSTANCE_ID = Column('INSTANCE ID', lambda j: j.instance_id)
CREATED = Column('CREATED', lambda j: j.state_changes[ExecutionState.CREATED].astimezone().replace(tzinfo=None))
EXEC_TIME = Column('EXECUTION TIME', lambda j: execution_time(j))
PROGRESS = Column('PROGRESS', lambda j: progress(j))
STATE = Column('STATE', lambda j: j.state.name)


def print_jobs(job_instances, columns: Iterable[Column], show_header: bool):
    headers = [column.name for column in columns] if show_header else ()
    jobs_as_fields = [_job_to_fields(j, columns) for j in job_instances]
    print(tabulate(jobs_as_fields, headers=headers))


def _job_to_fields(j, columns: Iterable[Column]):
    return [column.value_fnc(j) for column in columns]


def execution_time(job_instance):
    state = job_instance.state
    if not state.is_executing():
        return 'N/A'

    utc_now = datetime.datetime.now(datetime.timezone.utc)
    state_change = job_instance.state_changes[state]
    return utc_now - state_change


def progress(job_instance):
    if not job_instance.progress:
        return 'N/A'

    max_length = 35
    return job_instance.progress[:max_length] + (job_instance.progress[max_length:] and '..')


def print_state_change(job_instance):
    print(f"{job_instance.job_id}@{job_instance.instance_id} -> {job_instance.state.name}")
