import datetime
from typing import Iterable

from beautifultable import BeautifulTable, enums
from prompt_toolkit.eventloop.dummy_contextvars import Token
from prompt_toolkit.formatted_text import FormattedText
from prompt_toolkit import print_formatted_text
from pypager.pager import Pager
from pypager.source import GeneratorSource
from pypager.style import ui_style
from tabulate import tabulate
from termcolor import colored

from taro import util
from taro.execution import ExecutionState


class Column:
    def __init__(self, name, value_fnc):
        self.name = name
        self.value_fnc = value_fnc


JOB_ID = Column('JOB ID', lambda j: j.job_id)
INSTANCE_ID = Column('INSTANCE ID', lambda j: j.instance_id)
CREATED = Column('CREATED', lambda j: _format_dt(j.lifecycle.changed(ExecutionState.CREATED)))
EXECUTED = Column('EXECUTED', lambda j: _format_dt(j.lifecycle.execution_started()))
ENDED = Column('ENDED', lambda j: _format_dt(j.lifecycle.execution_finished()))
EXEC_TIME = Column('EXECUTION TIME', lambda j: execution_time(j))
STATE = Column('STATE', lambda j: j.lifecycle.state().name)
PROGRESS = Column('PROGRESS', lambda j: progress(j))
PROGRESS_RESULT = Column('RESULT', lambda j: result(j))


def print_jobs(job_instances, columns: Iterable[Column], show_header: bool):
    table = BeautifulTable(max_width=160, default_alignment=enums.ALIGN_LEFT)
    table.set_style(BeautifulTable.STYLE_COMPACT)
    table.column_headers = [column.name for column in columns] if show_header else ()
    l = []
    for j in job_instances:
        table.append_row(_job_to_fields(j, _get_color(j), columns))
        l.append(_job_to_fields(j, _get_color(j), columns))
    gen = table.stream(iter(l), append=False)
    while True:
        try:
            # print(next(gen))
            print_formatted_text(FormattedText([(ui_style['standout'], next(gen))]))
        except StopIteration:
            return
    # p = Pager()
    # p.add_source(GeneratorSource(content_generator(gen)))
    # p.run()


def content_generator(gen):
    for l in gen:
        yield [("", l + '\n')]


def _get_color(job_instance):
    state = job_instance.lifecycle.state()
    if state.is_failure():
        return 'red'

    if state.is_terminal() and state != ExecutionState.COMPLETED:
        return 'yellow'

    return None


def _job_to_fields(j, color, columns: Iterable[Column]):
    return [colored(column.value_fnc(j), color) if color else column.value_fnc(j) for column in columns]


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
    if not text:
        return text
    return text[:limit] + (text[limit:] and '..')


def print_state_change(job_instance):
    print(f"{job_instance.job_id}@{job_instance.instance_id} -> {job_instance.lifecycle.state().name}")
