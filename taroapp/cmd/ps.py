import json

from pygments import highlight
from pygments.formatters.terminal import TerminalFormatter
from pygments.lexers.data import JsonLexer

import taro.client
from taro import dto, util
from taro.jobs.job import JobInfoCollection,JobInfo
from taroapp import printer
from taroapp.view import instance as view_inst


def run(args):
    if args.instance:
        jobs = JobInfoCollection()
        for j in taro.client.read_jobs_info():
            if j.matches(args.instance, job_matching_strategy=util.substring_match):
                jobs.append(j)
    else:
        jobs = JobInfoCollection(taro.client.read_jobs_info())

    if args.format == 'table':
        printer.print_table(jobs.jobs, view_inst.DEFAULT_COLUMNS, show_header=True, pager=False)
    elif args.format == 'json':
        print(json.dumps(dto.to_jobs_dto(jobs)))
    elif args.format == 'jsonp':
        json_str = json.dumps(dto.to_jobs_dto(jobs), indent=2)
        print(highlight(json_str, JsonLexer(), TerminalFormatter()))
    else:
        assert False, 'Unknown format: ' + args.format
