import json

from pygments import highlight
from pygments.formatters.terminal import TerminalFormatter
from pygments.lexers.data import JsonLexer

import taro.client
from taro import dto
from taroapp import ps
from taroapp.view import instance as view_inst


def run(args):
    jobs = taro.client.read_jobs_info(args.instance)
    if args.format == 'table':
        ps.print_table(jobs, view_inst.DEFAULT_COLUMNS, _colours, show_header=True, pager=False)
    elif args.format == 'json':
        print(json.dumps([dto.to_info_dto(job) for job in jobs]))
    elif args.format == 'jsonp':
        json_str = json.dumps([dto.to_info_dto(job) for job in jobs], indent=2)
        print(highlight(json_str, JsonLexer(), TerminalFormatter()))
    else:
        assert False, 'Unknown format: ' + args.format


def _colours(job_info):
    state = job_info.state

    if state.is_before_execution():
        return '#44aaff'

    if state.is_executing():
        return 'green'

    if state.is_failure():
        return 'red'

    if job_info.warnings:
        return 'orange'

    if state.is_incomplete():
        return 'grey'

    return ''
