import argparse
import logging
import sys

from taro import runner
from taro.job import Job
from taro.process import ProcessExecution


def parse_args(args):
    parser = argparse.ArgumentParser(description='Manage your jobs with Taro')
    subparsers = parser.add_subparsers(dest='command')

    common = argparse.ArgumentParser()
    common.add_argument('--log-level', type=str, default=logging.INFO)

    exec_parser = subparsers.add_parser('exec', parents=[common], description='Execute command', add_help=False)
    exec_parser.add_argument('--id', type=str, default='anonymous')
    exec_parser.add_argument('-t', '--timeout', type=int)
    exec_parser.add_argument('args', type=str, nargs=argparse.REMAINDER, help='an execution argument')

    return parser.parse_args(args)


def main(args):
    args = parse_args(args)
    configure_logging(args)

    if args.command == 'exec':
        run_exec(args)


def configure_logging(args):
    logging.basicConfig(
        stream=sys.stdout,
        level=args.log_level,
        format='%(asctime)s - %(levelname)-5s - %(name)s - %(message)s')


def run_exec(args):
    execution = ProcessExecution(args.args)
    job = Job(args.id, execution)
    runner.run(job)


if __name__ == '__main__':
    main(sys.argv[1:])
