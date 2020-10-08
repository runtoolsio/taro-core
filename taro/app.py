import sys

from taro import cli, cmd


def main_cli():
    main(None)


def main(args):
    args = cli.parse_args(args)
    cmd.run(args)


if __name__ == '__main__':
    main(sys.argv[1:])
