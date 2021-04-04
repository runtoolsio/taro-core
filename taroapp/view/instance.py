from taro import util
from taro.execution import ExecutionState
from taroapp.ps import Column, format_dt

JOB_ID = Column('JOB ID', 30, lambda j: j.job_id)
INSTANCE_ID = Column('INSTANCE ID', 23, lambda j: j.instance_id)
CREATED = Column('CREATED', 25, lambda j: format_dt(j.lifecycle.changed(ExecutionState.CREATED)))
EXECUTED = Column('EXECUTED', 25, lambda j: format_dt(j.lifecycle.execution_started()))
ENDED = Column('ENDED', 25, lambda j: format_dt(j.lifecycle.execution_finished()))
EXEC_TIME = Column('EXECUTION TIME', 18, lambda j: execution_time_str(j))
STATE = Column('STATE', max(len(s.name) for s in ExecutionState) + 2, lambda j: j.state.name)
WARNINGS = Column('WARNINGS', 40, lambda j: ', '.join("{}: {}".format(k, v) for k, v in j.warnings.items()))
STATUS = Column('STATUS', 50, lambda j: j.status or '')
RESULT = Column('RESULT', 50, lambda j: j.status or '')

DEFAULT_COLUMNS = [JOB_ID, INSTANCE_ID, CREATED, EXEC_TIME, STATE, WARNINGS, STATUS]


def execution_time_str(job_info):
    if not job_info.lifecycle.executed():
        return 'N/A'

    if job_info.state.is_executing():
        exec_time = util.utc_now() - job_info.lifecycle.execution_started()
    else:
        exec_time = job_info.lifecycle.execution_time()
    return util.format_timedelta(exec_time)
