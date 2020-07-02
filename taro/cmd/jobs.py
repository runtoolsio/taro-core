import itertools

from taro import cnf, persistence, ExecutionState, jfilter, ps
from taro.api import Client
from taro.jfilter import AllFilter
from taro.view import instance as view_inst


def run(args):
    cnf.init(args)

    jobs = []

    client = Client()
    try:
        jobs += client.read_jobs_info()
    finally:
        client.close()

    persistence.init()
    try:
        jobs += persistence.read_jobs(chronological=args.chronological)
    finally:
        persistence.close()

    columns = [view_inst.JOB_ID, view_inst.INSTANCE_ID, view_inst.CREATED, view_inst.ENDED, view_inst.EXEC_TIME,
               view_inst.STATE, view_inst.WARNINGS, view_inst.STATUS]
    sorted_jobs = sorted(jobs, key=lambda j: j.lifecycle.changed(ExecutionState.CREATED),
                         reverse=not args.chronological)
    job_filter = _build_job_filter(args)
    filtered_jobs = filter(job_filter, sorted_jobs)
    limited_jobs = itertools.islice(filtered_jobs, 0, args.lines or None)
    ps.print_table(limited_jobs, columns, show_header=True, pager=not args.no_pager)


def _build_job_filter(args):
    job_filter = AllFilter()
    if args.id:
        job_filter <<= jfilter.create_id_filter(args.id)
    if args.finished:
        job_filter <<= jfilter.finished_filter
    if args.today:
        job_filter <<= jfilter.today_filter
    if args.since:
        job_filter <<= jfilter.create_since_filter(args.since)
    if args.until:
        job_filter <<= jfilter.create_until_filter(args.until)

    return job_filter
