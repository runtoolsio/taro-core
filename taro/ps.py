import datetime
import re
from collections import namedtuple
from typing import Iterable, List, Dict

import itertools
from prompt_toolkit import print_formatted_text
from prompt_toolkit.formatted_text import FormattedText as FTxt
from pypager.pager import Pager
from pypager.source import GeneratorSource

from taro import util
from taro.execution import ExecutionState
from taro.util import iterates

Column = namedtuple('Column', 'name max_width value_fnc')

JOB_ID = Column('JOB ID', 30, lambda j: j.job_id)
INSTANCE_ID = Column('INSTANCE ID', 23, lambda j: j.instance_id)
CREATED = Column('CREATED', 25, lambda j: _format_dt(j.lifecycle.changed(ExecutionState.CREATED)))
EXECUTED = Column('EXECUTED', 25, lambda j: _format_dt(j.lifecycle.execution_started()))
ENDED = Column('ENDED', 25, lambda j: _format_dt(j.lifecycle.execution_finished()))
EXEC_TIME = Column('EXECUTION TIME', 18, lambda j: execution_time(j))
STATE = Column('STATE', max(len(s.name) for s in ExecutionState) + 2, lambda j: j.lifecycle.state().name)
PROGRESS = Column('PROGRESS', 25, lambda j: progress(j))
RESULT = Column('RESULT', 25, lambda j: result(j))


@iterates
def print_jobs(job_infos, columns: Iterable[Column], *, show_header: bool, pager: bool):
    gen = output_gen(job_infos, columns, show_header)

    if pager:
        p = Pager()
        p.add_source(GeneratorSource(line + [('', '\n')] for line in gen))
        p.run()
    else:
        while True:
            print_formatted_text(next(gen))


def output_gen(job_infos, columns: Iterable[Column], show_header: bool):
    """
    Table Representation:
        Each column has padding of size 1 from each side applied in both header and values
        Left padding is hardcoded in the format token " {:x}"
        Right padding is implemented by text limiting
        Columns are separated by one space
        Header/Values separator line is of the same length as the column
    Column Widths:
        First 50 rows are examined to find optimal width of the columns
    """
    job_iter = iter(job_infos)
    first_fifty = list(itertools.islice(job_iter, 50))
    column_width = _calc_widths(first_fifty, columns)
    f = " ".join(" {:" + str(column_width[c] - 1) + "}" for c in columns)

    if show_header:
        header_line = f.format(*(c.name for c in columns))
        yield FTxt([('bold', header_line)])
        separator_line = " ".join("-" * (column_width[c]) for c in columns)
        yield FTxt([('bold', separator_line)])

    for j in itertools.chain(first_fifty, job_iter):
        line = f.format(*(_limit_text(c.value_fnc(j), column_width[c] - 2) for c in columns))
        yield FTxt([(_get_color(j), line)])


def _calc_widths(job_infos, columns: Iterable[Column]):
    column_width = {c: len(c.name) + 2 for c in columns}  # +2 for left and right padding
    for j in job_infos:
        for c in columns:
            column_width[c] = max(column_width[c], min(len(c.value_fnc(j)) + 2, c.max_width))  # +2 for padding
    return column_width


def _get_color(job_info):
    state = job_info.lifecycle.state()

    if state.is_before_execution():
        return 'green'

    if state.is_executing():
        return '#44aaff'

    if state.is_failure():
        return 'red'

    if state.is_terminal() and state != ExecutionState.COMPLETED:
        return 'orange'

    return ''


def _format_dt(dt):
    if not dt:
        return 'N/A'

    return dt.astimezone().replace(tzinfo=None).isoformat(sep=' ', timespec='milliseconds')


def execution_time(job_info):
    if not job_info.lifecycle.executed():
        return 'N/A'

    if job_info.lifecycle.state().is_executing():
        exec_time = datetime.datetime.now(datetime.timezone.utc) - job_info.lifecycle.execution_started()
    else:
        exec_time = job_info.lifecycle.last_changed() - job_info.lifecycle.execution_started()
    return util.format_timedelta(exec_time)


def progress(job_info):
    if job_info.lifecycle.state().is_terminal():
        return 'N/A'

    return _limit_text(job_info.progress, 35) or ''


def result(job_info):
    if not job_info.lifecycle.state().is_terminal():
        return 'N/A'

    return _limit_text(job_info.progress, 35) or ''


def _limit_text(text, limit):
    if not text or len(text) <= limit:
        return text
    return text[:limit - 2] + '..'


def print_state_change(job_info):
    print(f"{job_info.job_id}@{job_info.instance_id} -> {job_info.lifecycle.state().name}")


def parse_jobs_table(output, columns) -> List[Dict[Column, str]]:
    """
    Parses individual job lines from provided string containing job table (must include both header and sep line).
    Columns of the table must be specified.

    :param output: output containing job table
    :param columns: exact columns of the job table in correct order
    :return: list of dictionaries where each dictionary represent one job line by column -> value mapping
    """

    lines = [line for line in output.splitlines() if line]  # Ignore empty lines
    header_idx = [i for i, line in enumerate(lines) if all(column.name in line for column in columns)]
    if not header_idx:
        raise ValueError('The output does not contain specified job table')
    column_sep_line = lines[header_idx[0] + 1]  # Line separating header and values..
    sep_line_pattern = re.compile('-+')  # ..consisting of `-` strings for each column
    column_spans = [column.span() for column in sep_line_pattern.finditer(column_sep_line)]
    return [dict(zip(columns, (line[slice(*span)].strip() for span in column_spans)))
            for line in lines[header_idx[0] + 2:]]
