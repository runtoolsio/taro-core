from taro import client


def run(args):
    client.release_jobs(args.pending)
