from taroapp import ps
from taroapp.ps import Column

JOB_ID = Column('DISABLED JOB ID', 30, lambda dj: dj.job_id)
REGEX = Column('REGEX', 30, lambda dj: 'yes' if dj.regex else 'no')
DISABLED = Column('DISABLED', 30, lambda dj: ps.format_dt(dj.created))

DEFAULT_COLUMNS = [JOB_ID, REGEX, DISABLED]
