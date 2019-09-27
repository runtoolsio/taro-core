import sys

from taro import cli
from taro import configuration
from taro import log
from taro import runner
from taro.job import Job
from taro.process import ProcessExecution
from taro.util import get_attr, set_attr


def main(args):
    args = cli.parse_args(args)
    config = configuration.read_config()
    override_config(args, config)
    setup_logging(config.log)

    if args.action == 'exec':
        run_exec(args)


def override_config(args, config):
    arg_to_config = {
        'log_disabled': 'log.disable',
        'log_file': 'log.file.level',
        'log_file_path': 'log.file.path',
        'log_stdout': 'log.stdout.level'
    }

    for arg, conf in arg_to_config.items():
        arg_value = getattr(args, arg)
        if arg_value is not None:
            set_attr(config, conf.split('.'), arg_value)


def setup_logging(log_config):
    stdout_level = get_attr(log_config, 'stdout.level', none='off')
    if stdout_level.lower() != 'off':
        log.setup_console(stdout_level.lower())
    file_level = get_attr(log_config, 'file.level', none='off')
    if file_level.lower() != 'off':
        log.setup_file(file_level.lower())


def run_exec(args):
    execution = ProcessExecution([args.command] + args.arg)
    job = Job(args.id, execution)
    runner.run(job)


if __name__ == '__main__':
    main(sys.argv[1:])

# main(['exec', '--log-disabled', 'ls', '-l'])
