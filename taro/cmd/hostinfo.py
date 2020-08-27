import taro.hostinfo


def run(args):
    # Use `show` argument: taro hostinfo show
    host_info = taro.hostinfo.read_hostinfo()
    for name, value in host_info.items():
        print(f"{name}: {value}")
