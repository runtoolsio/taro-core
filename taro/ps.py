import itertools
import os
import re
from collections import namedtuple
from typing import List, Dict

from prompt_toolkit import print_formatted_text
from prompt_toolkit.formatted_text import FormattedText as FTxt
from pypager.pager import Pager
from pypager.source import GeneratorSource

from taro import util
from taro.util import iterates

Column = namedtuple('Column', 'name max_width value_fnc')


@iterates
def print_table(items, columns: List[Column], *, show_header: bool, pager: bool):
    gen = output_gen(items, columns, show_header, stretch_last_column=pager)

    if pager:
        p = Pager()
        p.add_source(GeneratorSource(line + [('', '\n')] for line in gen))
        p.run()
    else:
        while True:
            print_formatted_text(next(gen))


def output_gen(items, columns: List[Column], show_header: bool, stretch_last_column: bool):
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
    job_iter = iter(items)
    first_fifty = list(itertools.islice(job_iter, 50))
    column_width = _calc_widths(first_fifty, columns, stretch_last_column)
    f = " ".join(" {:" + str(column_width[c] - 1) + "}" for c in columns)

    if show_header:
        header_line = f.format(*(c.name for c in columns))
        yield FTxt([('bold', header_line)])
        separator_line = " ".join("-" * (column_width[c]) for c in columns)
        yield FTxt([('bold', separator_line)])

    for j in itertools.chain(first_fifty, job_iter):
        line = f.format(*(_limit_text(c.value_fnc(j), column_width[c] - 2) for c in columns))
        yield FTxt([(_get_color(j), line)])


def _calc_widths(items, columns: List[Column], stretch_last_column: bool):
    column_width = {c: len(c.name) + 2 for c in columns}  # +2 for left and right padding
    for i in items:
        for c in columns:
            column_width[c] = max(column_width[c], min(len(c.value_fnc(i)) + 2, c.max_width))  # +2 for padding

    if stretch_last_column:
        # vv Add spare terminal length to the last column vv
        try:
            terminal_length = os.get_terminal_size().columns
        except OSError:
            return column_width  # Failing in tests

        actual_length = sum(column_width.values()) + len(column_width)
        spare_length = terminal_length - actual_length
        if spare_length > 0:
            column_width[columns[-1]] += spare_length

    return column_width


def _get_color(job_info):
    if not hasattr(job_info, 'state'):  # TODO redesign
        return ''
    state = job_info.state

    if state.is_before_execution():
        return 'green'

    if state.is_executing():
        return '#44aaff'

    if state.is_failure():
        return 'red'

    if state.is_unexecuted() or job_info.warnings:
        return 'orange'

    return ''


def format_dt(dt):
    if not dt:
        return 'N/A'

    return dt.astimezone().replace(tzinfo=None).isoformat(sep=' ', timespec='milliseconds')


def execution_time(job_info):
    if not job_info.lifecycle.executed():
        return 'N/A'

    if job_info.state.is_executing():
        exec_time = util.utc_now() - job_info.lifecycle.execution_started()
    else:
        exec_time = job_info.lifecycle.execution_time()
    return util.format_timedelta(exec_time)


def _limit_text(text, limit):
    if not text or len(text) <= limit:
        return text
    return text[:limit - 2] + '..'


def print_state_change(job_info):
    print(f"{job_info.job_id}@{job_info.instance_id} -> {job_info.state.name}")


def parse_table(output, columns) -> List[Dict[Column, str]]:
    """
    Parses individual lines from provided string containing ps table (must include both header and sep line).
    Columns of the table must be specified.

    :param output: output containing ps table
    :param columns: exact columns of the ps table in correct order
    :return: list of dictionaries where each dictionary represent one item line by column -> value mapping
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
