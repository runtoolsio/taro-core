import taro.util
from taro.client import JobsClient
from taroapp import printer, style
from taroapp.printer import print_styled
from taroapp.view.instance import JOB_ID, INSTANCE_ID, CREATED, STATE


def run(args):
    with JobsClient() as client:
        all_jobs = client.read_jobs_info()
        jobs = [job for job in all_jobs if any(1 for args_id in args.ids if job.matches(args_id))]

        if not jobs:
            print('No instances to stop: ' + " ".join(args.ids))
            exit(1)

        if not args.force:
            printer.print_table(jobs, [JOB_ID, INSTANCE_ID, CREATED, STATE], show_header=True, pager=False)
            if not taro.util.cli_confirmation():
                exit(0)

        for args_id in args.ids:
            for id_, result in client.stop_jobs(args_id):
                print_styled(*style.job_instance_id_styled(*id_) + [('', ' -> '), ('', result)])
