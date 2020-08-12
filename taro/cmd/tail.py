from taro import client


def run(args):
    for instance, tail in client.read_tail(None):
        print(instance + ':')
        for line in tail:
            print(line)
