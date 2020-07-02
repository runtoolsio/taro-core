from taro.api import Client


def run(args):
    client = Client()
    try:
        client.release_jobs(args.pending)
    finally:
        client.close()
