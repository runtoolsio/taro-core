import logging

import sys

from taro import cli, cnf, http, hostinfo, cmd

logger = logging.getLogger(__name__)


def main_cli():
    main(None)


def main(args):
    args = cli.parse_args(args)
    # cmd.run(args)

    if args.action == cli.ACTION_EXEC:
        cmd.run(args)
    elif args.action == cli.ACTION_PS:
        cmd.run(args)
    elif args.action == cli.ACTION_JOBS:
        cmd.run(args)
    elif args.action == cli.ACTION_RELEASE:
        cmd.run(args)
    elif args.action == cli.ACTION_LISTEN:
        cmd.run(args)
    elif args.action == cli.ACTION_WAIT:
        cmd.run(args)
    elif args.action == cli.ACTION_STOP:
        cmd.run(args)
    elif args.action == cli.ACTION_DISABLE:
        cmd.run(args)
    elif args.action == cli.ACTION_LIST_DISABLED:
        cmd.run(args)
    elif args.action == cli.ACTION_HTTP:
        run_http(args)
    elif args.action == cli.ACTION_CONFIG:
        if args.config_action == cli.ACTION_CONFIG_SHOW:
            run_show_config(args)
    elif args.action == cli.ACTION_HOSTINFO:
        run_hostinfo()


def run_http(args):
    http.run(args.url, args.data, args.monitor_url, args.is_running, args.status)


def run_show_config(args):
    cnf.print_config(args)


def run_hostinfo():
    host_info = hostinfo.read_hostinfo()
    for name, value in host_info.items():
        print(f"{name}: {value}")


if __name__ == '__main__':
    main(sys.argv[1:])
