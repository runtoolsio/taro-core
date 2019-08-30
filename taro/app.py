import argparse
import logging
import sys

from taro import log
from taro import runner
from taro.job import Job
from taro.process import ProcessExecution


def parse_args(args):
    parser = argparse.ArgumentParser(description='Manage your jobs with Taro')
    subparsers = parser.add_subparsers(dest='action')

    common = argparse.ArgumentParser()
    common.add_argument('--log-level', type=str, default=logging.INFO)  # Remove default
    # https://bugs.python.org/issue14074
    common.add_argument('--log', nargs='+', action='append', type=str, metavar='<arg>',
                        help='<logger> [level] [arg] ...', default=[])

    exec_parser = subparsers.add_parser('exec', parents=[common], description='Execute command', add_help=False)
    exec_parser.add_argument('--id', type=str, default='anonymous')
    exec_parser.add_argument('-t', '--timeout', type=int)
    exec_parser.add_argument('-c', '--command', type=str, nargs='+', help='an execution argument', required=True)

    return parser.parse_args(args)


def main(args):
    args = parse_args(args)
    log.configure(args)

    if args.action == 'exec':
        run_exec(args)


def run_exec(args):
    execution = ProcessExecution(args.command)
    job = Job(args.id, execution)
    runner.run(job)


if __name__ == '__main__':
    main(sys.argv[1:])
