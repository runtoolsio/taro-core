import datetime
from collections import namedtuple
from typing import Iterable

from prompt_toolkit import print_formatted_text
from prompt_toolkit.formatted_text import FormattedText as FTxt
from pypager.pager import Pager
from pypager.source import GeneratorSource

from taro import util
from taro.execution import ExecutionState

Column = namedtuple('Column', 'name width value_fnc')

JOB_ID = Column('JOB ID', 15, lambda j: j.job_id)
INSTANCE_ID = Column('INSTANCE ID', 22, lambda j: j.instance_id)
CREATED = Column('CREATED', 24, lambda j: _format_dt(j.lifecycle.changed(ExecutionState.CREATED)))
EXECUTED = Column('EXECUTED', 24, lambda j: _format_dt(j.lifecycle.execution_started()))
ENDED = Column('ENDED', 24, lambda j: _format_dt(j.lifecycle.execution_finished()))
EXEC_TIME = Column('EXECUTION TIME', 17, lambda j: execution_time(j))
STATE = Column('STATE', max(len(s.name) for s in ExecutionState) + 1, lambda j: j.lifecycle.state().name)
PROGRESS = Column('PROGRESS', 10, lambda j: progress(j))
PROGRESS_RESULT = Column('RESULT', 20, lambda j: result(j))


def output_gen(job_instances, columns: Iterable[Column], show_header: bool):
    f = " ".join(" {:" + str(c.width) + "}" for c in columns)

    if show_header:
        header_line = f.format(*(c.name for c in columns))
        yield FTxt([('bold', header_line)])
        separator_line = " ".join("-" * (c.width + 1) for c in columns)
        yield FTxt([('bold', separator_line)])

    for j in job_instances:
        line = f.format(*(_limit_text(c.value_fnc(j), c.width - 1) for c in columns))
        yield FTxt([(_get_color(j), line)])


def print_jobs(job_instances, columns: Iterable[Column], *, show_header: bool, pager: bool):
    gen = output_gen(job_instances, columns, show_header)

    if pager:
        p = Pager()
        p.add_source(GeneratorSource(line + [('', '\n')] for line in gen))
        p.run()
    else:
        while True:
            try:
                print_formatted_text(next(gen))
            except StopIteration:
                break


def _get_color(job_instance):
    state = job_instance.lifecycle.state()
    if state.is_failure():
        return 'red'

    if state.is_terminal() and state != ExecutionState.COMPLETED:
        return 'orange'

    return ''


def _format_dt(dt):
    if not dt:
        return 'N/A'

    return dt.astimezone().replace(tzinfo=None).isoformat(sep=' ', timespec='milliseconds')


def execution_time(job_instance):
    if not job_instance.lifecycle.executed():
        return 'N/A'

    if job_instance.lifecycle.state().is_executing():
        exec_time = datetime.datetime.now(datetime.timezone.utc) - job_instance.lifecycle.execution_started()
    else:
        exec_time = job_instance.lifecycle.last_changed() - job_instance.lifecycle.execution_started()
    return util.format_timedelta(exec_time)


def progress(job_instance):
    if job_instance.lifecycle.state().is_terminal():
        return 'N/A'

    return _limit_text(job_instance.progress, 35) or ''


def result(job_instance):
    if not job_instance.lifecycle.state().is_terminal():
        return 'N/A'

    return _limit_text(job_instance.progress, 35) or ''


def _limit_text(text, limit):
    if not text or len(text) <= limit:
        return text
    return text[:limit - 2] + '..'


def print_state_change(job_instance):
    print(f"{job_instance.job_id}@{job_instance.instance_id} -> {job_instance.lifecycle.state().name}")
