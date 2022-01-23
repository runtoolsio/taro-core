import os

import taro.util
from taro.client import JobsClient
from taroapp import printer
from taroapp.view.instance import JOB_ID, INSTANCE_ID, CREATED, STATE


def run(args):
    with JobsClient() as client:
        all_jobs = client.read_jobs_info()
        jobs = [job for job in all_jobs if job.matches(args.instance)]

        if not jobs:
            print('No such instance to stop: ' + args.instance)
            exit(1)

        if args.force:
            client.stop_jobs([job.instance_id for job in jobs], args.interrupt)
            return
        else:
            if len(jobs) > 1 and not args.all:
                print('No action performed, because the criteria matches more than one instance. '
                    'Use --all flag if you wish to stop them all:' + os.linesep)
                return
            printer.print_table(jobs, [JOB_ID, INSTANCE_ID, CREATED, STATE], show_header=True, pager=False)
            if taro.util.cli_confirmation():
                client.stop_jobs([job.instance_id for job in jobs], args.interrupt)
                print("Successfully stopped")
            return  # Exit code non-zero?
