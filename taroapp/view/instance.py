from taro import util
from taro.jobs.execution import ExecutionState
from taro.util import format_dt_local_tz
from taroapp.printer import Column
from taroapp.style import general_style, job_style, instance_style, warn_style, job_state_style

JOB_ID = Column('JOB ID', 30, lambda j: j.job_id, job_style)
INSTANCE_ID = Column('INSTANCE ID', 23, lambda j: j.instance_id, instance_style)
PARAMETERS = Column('PARAMETERS', 23, lambda j: ', '.join("{}={}".format(k, v) for k, v in j.user_params.items()),
                    general_style)
CREATED = Column('CREATED', 25, lambda j: format_dt_local_tz(j.lifecycle.changed(ExecutionState.CREATED)),
                 general_style)
EXECUTED = Column('EXECUTED', 25, lambda j: format_dt_local_tz(j.lifecycle.execution_started, null='N/A'),
                  general_style)
ENDED = Column('ENDED', 25, lambda j: format_dt_local_tz(j.lifecycle.execution_finished, null='N/A'), general_style)
EXEC_TIME = Column('TIME', 18, lambda j: execution_time_str(j), general_style)
STATE = Column('STATE', max(len(s.name) for s in ExecutionState) + 2, lambda j: j.state.name, job_state_style)
WARNINGS = Column('WARNINGS', 40, lambda j: ', '.join("{}: {}".format(k, v) for k, v in j.warnings.items()), warn_style)
STATUS = Column('STATUS', 50, lambda j: j.status or '', general_style)
RESULT = Column('RESULT', 50, lambda j: j.status or '', general_style)

DEFAULT_COLUMNS = [JOB_ID, INSTANCE_ID, CREATED, EXEC_TIME, STATE, WARNINGS, STATUS]


def execution_time_str(job_info):
    if not job_info.lifecycle.executed():
        return 'N/A'

    return util.format_timedelta(job_info.lifecycle.execution_time, show_ms=False)
