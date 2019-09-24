import sys

from taro import cli
from taro import configuration
from taro import log
from taro import runner
from taro.job import Job
from taro.process import ProcessExecution


def main(args):
    args = cli.parse_args(args)
    config = configuration.read_config()
    args_config = merge_args_config(args, config)
    print(args_config)
    setup_logging(args)

    if args.action == 'exec':
        run_exec(args)


def merge_args_config(args, config):
    merged = dict(config)
    for k, v in vars(args).items():
        if v:
            merged[k] = v
    return merged


def setup_logging(args):
    if args.log_stdout and args.log_stdout.lower() != 'off':
        log.setup_console(args.log_stdout.lower())
    if args.log_file and args.log_file.lower() != 'off':
        log.setup_file(args.log_file.lower())


def run_exec(args):
    execution = ProcessExecution([args.command] + args.arg)
    job = Job(args.id, execution)
    runner.run(job)


if __name__ == '__main__':
    main(sys.argv[1:])
