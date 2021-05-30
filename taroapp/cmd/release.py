import taro.client


def run(args):
    taro.client.release_jobs(args.pending)
