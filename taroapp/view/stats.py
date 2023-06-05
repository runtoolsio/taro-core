from taro import util
from taro.jobs.execution import ExecutionState
from taro.util import format_dt_local_tz
from taroapp.printer import Column
from taroapp.style import stats_style, job_id_stats_style, stats_state_style

JOB_ID = Column('JOB ID', 30, lambda s: s.job_id, job_id_stats_style)
ENDED = Column('ENDED', 10, lambda s: str(s.count), stats_style)
FIRST_CREATED = Column('FIRST INSTANCE', 25, lambda s: format_dt_local_tz(s.first_created), stats_style)
LAST_CREATED = Column('LAST INSTANCE', 25, lambda s: format_dt_local_tz(s.last_created), stats_style)
FASTEST = Column('FASTEST', 18, lambda s: util.format_timedelta(s.fastest_time, show_ms=False), stats_style)
AVERAGE = Column('AVERAGE', 18, lambda s: util.format_timedelta(s.average_time, show_ms=False), stats_style)
SLOWEST = Column('SLOWEST', 18, lambda s: util.format_timedelta(s.slowest_time, show_ms=False), stats_style)
LAST_TIME = Column('LAST', 18, lambda s: util.format_timedelta(s.last_time, show_ms=False), stats_style)
STATE = Column('LAST STATE', max(len(s.name) for s in ExecutionState) + 2, lambda s: s.last_state.name, stats_state_style)
FAILED = Column('FAILED', 10, lambda s: str(s.failed_count), stats_style)
WARN = Column('WARNINGS', 10, lambda s: str(s.warning_count), stats_style)

DEFAULT_COLUMNS =\
    [JOB_ID, ENDED, FIRST_CREATED, LAST_CREATED, FASTEST, AVERAGE, SLOWEST, LAST_TIME, STATE, FAILED, WARN]
