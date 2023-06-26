import json

from pygments import highlight
from pygments.formatters.terminal import TerminalFormatter
from pygments.lexers.data import JsonLexer

import taro.client
from taro.jobs.inst import JobInstances
from taro.util import MatchingStrategy
from taroapp import printer, argsutil
from taroapp.view import instance as view_inst


def run(args):
    instance_match = argsutil.instance_matching_criteria(args, MatchingStrategy.PARTIAL)
    job_instances = taro.client.read_job_instances(instance_match).responses
    instances = JobInstances(job_instances)

    if args.format == 'table': 
        columns = view_inst.DEFAULT_COLUMNS
        if args.show_params:
            columns.insert(2, view_inst.PARAMETERS)
        printer.print_table(instances, columns, show_header=True, pager=False)
    elif args.format == 'json':
        print(json.dumps(instances.to_dict()))
    elif args.format == 'jsonp':
        json_str = json.dumps(instances.to_dict(), indent=2)
        print(highlight(json_str, JsonLexer(), TerminalFormatter()))
    else:
        assert False, 'Unknown format: ' + args.format
