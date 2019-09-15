import argparse
import sys

from taro import log
from taro import runner
from taro.job import Job
from taro.process import ProcessExecution


def parse_args(args):
    parser = argparse.ArgumentParser(description='Manage your jobs with Taro')
    common = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='action')  # command/action

    exec_parser = subparsers.add_parser('exec', parents=[common], description='Execute command', add_help=False)
    # exec_parser.add_argument('--log', nargs='+', action='append', type=str, metavar='<arg>',
    #                          help='<logger> [level] [arg] ...', default=[])
    # exec_parser.add_argument('--log-level', type=str, default=logging.INFO)  # Remove default
    # exec_parser.add_argument('-t', '--timeout', type=int)
    # # Terms command and arguments taken from python doc and docker run help,
    # # for this app these are operands (alternatively arguments)
    exec_parser.add_argument('--dry-run', action='store_true')  # TODO
    exec_parser.add_argument('--id', type=str, default='anonymous', help='job ID')
    exec_parser.add_argument('--log-file', nargs=1, type=str, metavar='<log-level>',
                             help='log into taro.log file with given <log-level>')
    # Terms command and arguments taken from python doc and docker run help,
    # for this app (or rather exec command) these are operands (alternatively arguments)
    exec_parser.add_argument('command', type=str, metavar='COMMAND', help='program to execute')
    exec_parser.add_argument('arg', type=str, metavar='ARG', nargs=argparse.REMAINDER, help="program arguments")

    return parser.parse_args(args)


def main(args):
    args = parse_args(args)
    setup_logging(args)

    if args.action == 'exec':
        run_exec(args)


def setup_logging(args):
    if args.log_file and args.log_file[0].lower() != 'off':
        log.setup_file(args.log_file[0].lower())


def run_exec(args):
    execution = ProcessExecution([args.command] + args.arg)
    job = Job(args.id, execution)
    runner.run(job)


if __name__ == '__main__':
    main(sys.argv[1:])
