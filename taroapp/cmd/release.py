import taro.client
from taro import ExecutionState
from taro.util import MatchingStrategy
from taroapp import argsutil


def run(args):
    instance_match = argsutil.instance_matching_criteria(args, MatchingStrategy.FN_MATCH)
    if args.pending:
        responses, _ = taro.client.release_pending_jobs(args.pending, instance_match)
    elif args.queued:
        if not instance_match:
            raise ValueError("Instance must be specified when releasing queued")
        responses, _ = taro.client.release_waiting_jobs(instance_match, ExecutionState.QUEUED)
    else:
        assert False, "Missing release option"

    if responses:
        print('Released:')
        for released_resp in responses:
            print(released_resp.instance_metadata.id)
