import taro.client
from taro.util import MatchingStrategy
from taroapp import argsutil


def run(args):
    instance_match = argsutil.instance_matching_criteria(args, MatchingStrategy.FN_MATCH)
    responses, _ = taro.client.release_jobs(args.pending[0], instance_match)
    if responses:
        print('Released:')
        for released_resp in responses:
            print(released_resp.instance_metadata.id)
