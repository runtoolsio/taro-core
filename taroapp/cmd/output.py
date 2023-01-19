from taro import client, JobInfo
from taro.jobs.job import InstanceMatchingCriteria, IDMatchingCriteria
from taro.util import MatchingStrategy


def run(args):
    instance_match = InstanceMatchingCriteria(IDMatchingCriteria(args.instance, MatchingStrategy.PARTIAL))
    instances, _ = client.read_jobs_info(instance_match)
    if instances:
        sorted_instances = sorted(instances, key=JobInfo.created, reverse=True)
        for line in sorted_instances[0].error_output:
            print(line)
