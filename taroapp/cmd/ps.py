import json

from pygments import highlight
from pygments.formatters.terminal import TerminalFormatter
from pygments.lexers.data import JsonLexer

import taro.client
from taro import dto
from taro.jobs.job import JobInfoCollection
from taroapp import ps
from taroapp.view import instance as view_inst


def run(args):
    jobs = JobInfoCollection(*taro.client.read_jobs_info(args.instance))
    if args.format == 'table':
        ps.print_table(jobs.jobs, view_inst.DEFAULT_COLUMNS, show_header=True, pager=False)
    elif args.format == 'json':
        print(json.dumps(dto.to_jobs_dto(jobs)))
    elif args.format == 'jsonp':
        json_str = json.dumps(dto.to_jobs_dto(jobs), indent=2)
        print(highlight(json_str, JsonLexer(), TerminalFormatter()))
    else:
        assert False, 'Unknown format: ' + args.format
