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
            print('No instance to stop: ' + args.instance)
            exit(1)

        if not args.force:
            printer.print_table(jobs, [JOB_ID, INSTANCE_ID, CREATED, STATE], show_header=True, pager=False)
            if not taro.util.cli_confirmation():
                exit(0)

        inst_results = client.stop_jobs([job.instance_id for job in jobs], args.interrupt)
        for inst_res in inst_results:
            print(f"{inst_res[0]} -> {inst_res[1]}")
