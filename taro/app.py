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
    override_config(config, args)
    setup_logging(args)

    if args.action == 'exec':
        run_exec(args)


def override_config(config, args):
    arg_to_config = {
        'log_disabled': 'log.disable',
        'log_file': 'log.file.level',
        'log_file_path': 'log.file.path',
        'log_stdout': 'log.stdout.level'
    }

    for arg, conf in arg_to_config.items():
        arg_value = getattr(args, arg)
        if arg_value is not None:
            _setattr(config, conf.split('.'), arg_value)


def _setattr(obj, fields, value):
    if len(fields) == 1:
        setattr(obj, fields[0], value)
    else:
        _setattr(getattr(obj, fields[0]), fields[1:], value)


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
