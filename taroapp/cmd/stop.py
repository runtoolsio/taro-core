import taro.util
from taro.client import JobsClient
from taro.util import MatchingStrategy
from taroapp import printer, style, argsutil
from taroapp.printer import print_styled
from taroapp.view.instance import JOB_ID, INSTANCE_ID, CREATED, STATE


def run(args):
    with JobsClient() as client:
        instance_match = argsutil.instance_matching_criteria(args, MatchingStrategy.FN_MATCH)
        stop_jobs, _ = client.read_jobs_info(instance_match)

        if not stop_jobs:
            print('No instances to stop: ' + " ".join(args.instances))
            exit(1)

        if not args.force:
            printer.print_table(stop_jobs, [JOB_ID, INSTANCE_ID, CREATED, STATE], show_header=True, pager=False)
            if not taro.util.cli_confirmation():
                exit(0)

        for stop_resp in client.stop_jobs(instance_match).responses:
            print_styled(*style.job_instance_id_styled(stop_resp.id) + [('', ' -> '), ('', stop_resp.result_str)])
