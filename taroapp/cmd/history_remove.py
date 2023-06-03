from taro.jobs import persistence
from taro.jobs.job import InstanceMatchingCriteria
from taro.util import MatchingStrategy
from taroapp import cliutil


def run(args):
    total = 0
    for instance in args.instances:
        instance_match = InstanceMatchingCriteria.parse_pattern(instance, MatchingStrategy.FN_MATCH)
        count = persistence.count_instances(instance_match=instance_match)
        print(str(count) + " records found for " + instance)
        total += count

    if not (total and cliutil.user_confirmation(yes_on_empty=True, catch_interrupt=True)):
        print('Skipped..')
        return

    for instance in args.instances:
        instance_match = InstanceMatchingCriteria.parse_pattern(instance, MatchingStrategy.FN_MATCH)
        persistence.remove_instances(instance_match)
