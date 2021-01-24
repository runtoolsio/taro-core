from taro import cnf, persistence, jfilter, ps
from taro.jfilter import AllFilter
from taro.persistence import SortCriteria
from taro.view import instance as view_inst


def run(args):
    cnf.init(args)
    persistence.init()

    try:
        jobs = persistence.read_jobs(
            sort=SortCriteria[args.sort.upper()], asc=args.asc, limit=args.lines or -1)
    finally:
        persistence.close()

    columns = [view_inst.JOB_ID, view_inst.INSTANCE_ID, view_inst.CREATED, view_inst.ENDED, view_inst.EXEC_TIME,
               view_inst.STATE, view_inst.WARNINGS, view_inst.RESULT]
    job_filter = _build_job_filter(args)
    filtered_jobs = filter(job_filter, jobs)
    ps.print_table(filtered_jobs, columns, _colours, show_header=True, pager=not args.no_pager)


def _build_job_filter(args):
    job_filter = AllFilter()
    if args.id:
        job_filter <<= jfilter.create_id_filter(args.id)
    if args.today:
        job_filter <<= jfilter.today_filter
    if args.since:
        job_filter <<= jfilter.create_since_filter(args.since)
    if args.until:
        job_filter <<= jfilter.create_until_filter(args.until)

    return job_filter


def _colours(job_info):
    state = job_info.state

    if state.is_failure():
        return 'red'

    if state.is_unexecuted() or job_info.warnings:
        return 'orange'

    return ''  # white
