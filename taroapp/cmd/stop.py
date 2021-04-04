import os

from taroapp import ps
from taro.api import Client
from taroapp.view.instance import JOB_ID, INSTANCE_ID, CREATED, STATE


def run(args):
    with Client() as client:
        all_jobs = client.read_jobs_info()
        jobs = [job for job in all_jobs if job.matches(args.instance)]

        if not jobs:
            print('No such instance to stop: ' + args.instance)
            exit(1)

        if len(jobs) > 1 and not args.all:
            print('No action performed, because the criteria matches more than one instance. '
                  'Use --all flag if you wish to stop them all:' + os.linesep)
            ps.print_table(jobs, [JOB_ID, INSTANCE_ID, CREATED, STATE], show_header=True, pager=False)
            return  # Exit code non-zero?

        inst_results = client.stop_jobs([job.instance_id for job in jobs], args.interrupt)
        for i_res in inst_results:
            print(f"{i_res[0]} -> {i_res[1]}")
