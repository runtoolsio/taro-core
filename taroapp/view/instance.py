from taro import util
from taro.jobs.execution import ExecutionState
from taro.util import format_dt_local_tz
from taroapp.printer import Column
from taroapp.style import general_style, job_id_style, instance_style, warn_style, job_state_style

JOB_ID = Column('JOB ID', 30, lambda j: j.job_id, job_id_style)
INSTANCE_ID = Column('INSTANCE ID', 23, lambda j: j.instance_id, instance_style)
PARAMETERS = Column('PARAMETERS', 23,
                    lambda j: ', '.join("{}={}".format(k, v) for k, v in j.metadata.user_params.items()), general_style)
CREATED = Column('CREATED', 25, lambda j: format_dt_local_tz(j.lifecycle.created_at), general_style)
EXECUTED = Column('EXECUTED', 25, lambda j: format_dt_local_tz(j.lifecycle.executed_at, null='N/A'), general_style)
ENDED = Column('ENDED', 25, lambda j: format_dt_local_tz(j.lifecycle.ended_at, null='N/A'), general_style)
EXEC_TIME = Column('TIME', 18, lambda j: util.format_timedelta(j.lifecycle.execution_time, show_ms=False, null='N/A'),
                   general_style)
STATE = Column('STATE', max(len(s.name) for s in ExecutionState) + 2, lambda j: j.state.name, job_state_style)
WARNINGS = Column('WARNINGS', 40, lambda j: ', '.join("{}: {}".format(k, v) for k, v in j.warnings.items()), warn_style)
STATUS = Column('STATUS', 50, lambda j: j.status or '', general_style)
RESULT = Column('RESULT', 50, lambda j: j.status or '', general_style)

DEFAULT_COLUMNS = [JOB_ID, INSTANCE_ID, CREATED, EXEC_TIME, STATE, WARNINGS, STATUS]
