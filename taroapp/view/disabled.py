import taro.util.dt
from taroapp.printer import Column
# TODO delete
JOB_ID = Column('DISABLED JOB ID', 30, lambda dj: dj.job_id, lambda _: "")
REGEX = Column('REGEX', 30, lambda dj: 'yes' if dj.regex else 'no', lambda _: "")
DISABLED = Column('DISABLED', 30, lambda dj: taro.util.dt.format_dt_ms_local_tz(dj.created), lambda _: "")

DEFAULT_COLUMNS = [JOB_ID, REGEX, DISABLED]
