import taro.hostinfo


def run(args):
    host_info = taro.hostinfo.read_hostinfo()
    for name, value in host_info.items():
        print(f"{name}: {value}")
