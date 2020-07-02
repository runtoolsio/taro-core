import taro.http


def run(args):
    taro.http.run(args.url, args.data, args.monitor_url, args.is_running, args.status)
