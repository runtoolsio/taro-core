import taro.util
from taro.jobs import persistence
from taro.jobs.job import InstanceMatchingCriteria, IDMatchingCriteria
from taro.util import MatchingStrategy


def run(args):
    print("Jobs to be removed:")
    total = 0
    for instance in args.instances:
        instance_match = InstanceMatchingCriteria(IDMatchingCriteria([instance], MatchingStrategy.FN_MATCH))
        count = persistence.num_of_job(instance_match=instance_match)
        print(str(count) + " records found for " + instance)
        total += count

    if total and taro.util.cli_confirmation(catch_interrupt=True):
        for instance in args.instances:
            instance_match = InstanceMatchingCriteria(IDMatchingCriteria([instance], MatchingStrategy.FN_MATCH))
            persistence.remove_jobs(instance_match)
