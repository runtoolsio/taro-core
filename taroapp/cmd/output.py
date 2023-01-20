from taro import client, JobInfo
from taro.jobs.job import InstanceMatchingCriteria, IDMatchingCriteria
from taro.util import MatchingStrategy
from taroapp.view import instance as view_inst


def run(args):
    instance_match = InstanceMatchingCriteria(IDMatchingCriteria(args.instance, MatchingStrategy.PARTIAL))
    instances, _ = client.read_jobs_info(instance_match)


    if instances:
        columns = [view_inst.JOB_ID, view_inst.INSTANCE_ID, view_inst.CREATED, view_inst.ENDED, view_inst.STATE]
        sorted_instances = sorted(instances, key=JobInfo.created, reverse=True)
        for line in sorted_instances[0].error_output:
            print(line)
