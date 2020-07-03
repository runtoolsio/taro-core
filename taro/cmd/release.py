from taro.api import Client


def run(args):
    with Client() as client:
        client.release_jobs(args.pending)
