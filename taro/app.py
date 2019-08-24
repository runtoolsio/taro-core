import sys
import argparse

from taro import runner
from taro.job import Job
from taro.process import ProcessExecution


def parse_args(args):
    parser = argparse.ArgumentParser(description='Manage your jobs with Taro')
    parser.add_argument('--debug', action='store_true')

    subparsers = parser.add_subparsers(dest='command')
    exec_parser = subparsers.add_parser('exec')
    exec_parser.add_argument('-t', '--timeout', type=int)
    exec_parser.add_argument('--id', type=str, default='anonymous')
    exec_parser.add_argument('args', type=str, nargs=argparse.REMAINDER, help='an execution argument')
    return parser.parse_args(args)


def main(args):
    args = parse_args(args)
    if args.command == 'exec':
        run_exec(args)


def run_exec(args):
    execution = ProcessExecution(args.args)
    job = Job(args.id, execution)
    runner.run(job)


if __name__ == '__main__':
    main(sys.argv[1:])
