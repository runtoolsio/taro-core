import taro.client


def run(args):
    responses, _ = taro.client.release_jobs(args.pending)
    if responses:
        print('Released:')
        for released_resp in responses:
            print(released_resp.instance_metadata.id)
