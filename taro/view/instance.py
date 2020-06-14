from taro import ExecutionState
from taro.ps import Column, format_dt, execution_time

JOB_ID = Column('JOB ID', 30, lambda j: j.job_id)
INSTANCE_ID = Column('INSTANCE ID', 23, lambda j: j.instance_id)
CREATED = Column('CREATED', 25, lambda j: format_dt(j.lifecycle.changed(ExecutionState.CREATED)))
EXECUTED = Column('EXECUTED', 25, lambda j: format_dt(j.lifecycle.execution_started()))
ENDED = Column('ENDED', 25, lambda j: format_dt(j.lifecycle.execution_finished()))
EXEC_TIME = Column('EXECUTION TIME', 18, lambda j: execution_time(j))
STATE = Column('STATE', max(len(s.name) for s in ExecutionState) + 2, lambda j: j.state.name)
WARNINGS = Column('WARNINGS', 40, lambda j: ', '.join([w.id for w in j.warnings]))
STATUS = Column('STATUS', 50, lambda j: j.status or '')

DEFAULT_COLUMNS = [JOB_ID, INSTANCE_ID, CREATED, EXEC_TIME, STATE, WARNINGS, STATUS]
