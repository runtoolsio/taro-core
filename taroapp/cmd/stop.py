from taro.client import JobsClient
from taro.util import MatchingStrategy
from taroapp import printer, style, argsutil, cliutil
from taroapp.printer import print_styled
from taroapp.view.instance import JOB_ID, INSTANCE_ID, CREATED, STATE


def run(args):
    with JobsClient() as client:
        instance_match = argsutil.instance_matching_criteria(args, MatchingStrategy.FN_MATCH)
        stop_jobs, _ = client.read_jobs_info(instance_match)

        if not stop_jobs:
            print('No instances to stop: ' + " ".join(args.instances))
            return

        if not args.force:
            print('Instances to stop:')
            printer.print_table(stop_jobs, [JOB_ID, INSTANCE_ID, CREATED, STATE], show_header=True, pager=False)
            if not cliutil.user_confirmation(yes_on_empty=True, catch_interrupt=True):
                return

        for stop_resp in client.stop_jobs(instance_match).responses:
            print_styled(*style.job_instance_id_styled(stop_resp.id) + [('', ' -> '), ('', stop_resp.result_str)])
