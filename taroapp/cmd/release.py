import taro.client


def run(args):
    released, _ = taro.client.release_jobs(args.pending)
    if released:
        print('Released:')
        for released_job in released:
            print(released_job)
