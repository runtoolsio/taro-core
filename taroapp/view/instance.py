from taro import util
from taro.jobs.execution import ExecutionState
from taro.theme import Theme
from taroapp.printer import Column, format_dt


def no_style(_):
    return ""


def warn_style(_):
    return Theme.warning


def state_style(job):
    if job.state.is_before_execution():
        return Theme.state_before_execution
    if job.state.is_executing():
        return Theme.state_executing
    if job.state.is_incomplete():
        return Theme.state_incomplete
    if job.state.is_unexecuted():
        return Theme.state_not_executed
    if job.state.is_failure():
        return Theme.state_failure
    return ""


JOB_ID = Column('JOB ID', 30, lambda j: j.job_id, no_style)
INSTANCE_ID = Column('INSTANCE ID', 23, lambda j: j.instance_id, no_style)
CREATED = Column('CREATED', 25, lambda j: format_dt(j.lifecycle.changed(ExecutionState.CREATED)), no_style)
EXECUTED = Column('EXECUTED', 25, lambda j: format_dt(j.lifecycle.execution_started()), no_style)
ENDED = Column('ENDED', 25, lambda j: format_dt(j.lifecycle.execution_finished()), no_style)
EXEC_TIME = Column('EXECUTION TIME', 18, lambda j: execution_time_str(j), no_style)
STATE = Column('STATE', max(len(s.name) for s in ExecutionState) + 2, lambda j: j.state.name, state_style)
WARNINGS = Column('WARNINGS', 40, lambda j: ', '.join("{}: {}".format(k, v) for k, v in j.warnings.items()), warn_style)
STATUS = Column('STATUS', 50, lambda j: j.status or '', no_style)
RESULT = Column('RESULT', 50, lambda j: j.status or '', no_style)

DEFAULT_COLUMNS = [JOB_ID, INSTANCE_ID, CREATED, EXEC_TIME, STATE, WARNINGS, STATUS]


def execution_time_str(job_info):
    if not job_info.lifecycle.executed():
        return 'N/A'

    if job_info.state.is_executing():
        exec_time = util.utc_now() - job_info.lifecycle.execution_started()
    else:
        exec_time = job_info.lifecycle.execution_time()
    return util.format_timedelta(exec_time)
