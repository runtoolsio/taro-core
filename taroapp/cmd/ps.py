import json
from numpy import insert

from pygments import highlight
from pygments.formatters.terminal import TerminalFormatter
from pygments.lexers.data import JsonLexer

import taro.client
from taro import dto, util
from taro.jobs.job import JobInfoCollection, JobInfo
from taroapp import printer
from taroapp.view import instance as view_inst


def run(args):
    jobs = [job for job in taro.client.read_jobs_info()
            if not args.instance or job.matches(args.instance, job_matching_strategy=util.substring_match)]
    jobs = JobInfoCollection(*jobs)

    if args.format == 'table': 
        columns = view_inst.DEFAULT_COLUMNS
        if args.show_params:
            columns.insert(2, view_inst.PARAMETERS)
        printer.print_table(jobs.jobs, columns, show_header=True, pager=False)
    elif args.format == 'json':
        print(json.dumps(dto.to_jobs_dto(jobs)))
    elif args.format == 'jsonp':
        json_str = json.dumps(dto.to_jobs_dto(jobs), indent=2)
        print(highlight(json_str, JsonLexer(), TerminalFormatter()))
    else:
        assert False, 'Unknown format: ' + args.format
