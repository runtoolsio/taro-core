import sys

from taro import cli
from taro import cnf
from taro import log
from taro import runner
from taro.job import Job
from taro.process import ProcessExecution
from taro.util import get_attr, set_attr


def main(args):
    args = cli.parse_args(args)
    config = cnf.read_config()
    override_config(args, config)
    setup_logging(config)

    if args.action == 'exec':
        run_exec(args)


def override_config(args, config):
    """
    Overrides values in configuration with cli option values for those specified on command line

    :param args: command line arguments
    :param config: configuration
    """

    arg_to_config = {
        'log_enabled': cnf.LOG_ENABLED,
        'log_stdout': cnf.LOG_STDOUT_LEVEL,
        'log_file': cnf.LOG_FILE_LEVEL,
        'log_file_path': cnf.LOG_FILE_PATH,
    }

    for arg, conf in arg_to_config.items():
        arg_value = getattr(args, arg)
        if arg_value is not None:
            set_attr(config, conf.split('.'), arg_value)


def setup_logging(config):
    if not get_attr(config, cnf.LOG_ENABLED, none=False):
        return

    stdout_level = get_attr(config, cnf.LOG_STDOUT_LEVEL, none='off').lower()
    if stdout_level != 'off':
        log.setup_console(stdout_level)

    file_level = get_attr(config, cnf.LOG_FILE_LEVEL, none='off').lower()
    if file_level != 'off':
        log.setup_file(file_level, get_attr(config, cnf.LOG_FILE_PATH))


def run_exec(args):
    execution = ProcessExecution([args.command] + args.arg)
    job = Job(args.id, execution)
    runner.run(job)


if __name__ == '__main__':
    main(sys.argv[1:])

# main(['exec', '--log-disabled', 'ls', '-l'])
