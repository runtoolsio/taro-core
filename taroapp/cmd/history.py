from taro.jobs import persistence
from taro.jobs.persistence import SortCriteria
from taro.util import MatchingStrategy
from taroapp import printer, argsutil
from taroapp.view import instance as view_inst


def run(args):
    instance_match = argsutil.instance_matching_criteria(args, MatchingStrategy.PARTIAL)
    if args.slowest:
        args.last = True
        args.sort = SortCriteria.TIME.name
        args.asc = False

    jobs = persistence.read_jobs(
        instance_match, SortCriteria[args.sort.upper()], asc=args.asc, limit=args.lines or -1, last=args.last)

    columns = [view_inst.JOB_ID, view_inst.INSTANCE_ID, view_inst.CREATED, view_inst.ENDED, view_inst.EXEC_TIME,
               view_inst.STATE, view_inst.WARNINGS, view_inst.RESULT]
    if args.show_params:
        columns.insert(2, view_inst.PARAMETERS)

    printer.print_table(jobs, columns, show_header=True, pager=not args.no_pager)
