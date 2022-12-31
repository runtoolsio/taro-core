from taro.jobs import persistence
from taro.jobs.persistence import SortCriteria
from taroapp import printer, jfilter
from taroapp.jfilter import AllFilter
from taroapp.view import instance as view_inst


def run(args):
    jobs = persistence.read_jobs(ids=args.ids, sort=SortCriteria[args.sort.upper()], asc=args.asc,
                                 limit=args.lines or -1, last=args.last)

    columns = [view_inst.JOB_ID, view_inst.INSTANCE_ID, view_inst.CREATED, view_inst.ENDED, view_inst.EXEC_TIME,
               view_inst.STATE, view_inst.WARNINGS, view_inst.RESULT]
    if args.show_params:
        columns.insert(2, view_inst.PARAMETERS)
    job_filter = _build_job_filter(args)
    filtered_jobs = filter(job_filter, jobs)
    printer.print_table(filtered_jobs, columns, show_header=True, pager=not args.no_pager)


def _build_job_filter(args):
    job_filter = AllFilter()
    # if args.id: TODO do we really need this?
    #     job_filter <<= jfilter.create_id_filter(args.id)
    if args.today:
        job_filter <<= jfilter.today_filter
    if args.yesterday:
        job_filter <<= jfilter.yesterday_filter
    if args.since:
        job_filter <<= jfilter.create_since_filter(args.since)
    if args.until:
        job_filter <<= jfilter.create_until_filter(args.until)

    return job_filter
