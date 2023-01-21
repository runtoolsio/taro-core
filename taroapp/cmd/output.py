import itertools

from taro import client, JobInfo
from taro.jobs import persistence
from taro.jobs.job import InstanceMatchingCriteria, IDMatchingCriteria
from taro.theme import Theme
from taro.util import MatchingStrategy
from taroapp import printer
from taroapp.view.instance import JOB_ID, INSTANCE_ID, CREATED, ENDED, STATE


def run(args):
    instance_match = InstanceMatchingCriteria(IDMatchingCriteria([args.instance], MatchingStrategy.PARTIAL))
    instances, _ = client.read_jobs_info(instance_match)

    if not instances:
        instances = persistence.read_jobs(instance_match)

    if not instances:
        print('No matching instance found')
        return

    columns = [JOB_ID, INSTANCE_ID, CREATED, ENDED, STATE]
    instance = sorted(instances, key=JobInfo.created, reverse=True)[0]
    footer_gen = itertools.chain(
        (('', ''), (Theme.warning, 'Error output:')),
        (['', err] for err in instance.error_output)
    )
    printer.print_table([instance], columns, show_header=True, pager=not args.no_pager, footer=footer_gen)
