import os
import sys

from taro import cli, paths, cnf, log, runner
from taro.job import Job
from taro.process import ProcessExecution
from taro.util import get_attr, set_attr


def main(args):
    args = cli.parse_args(args)

    if args.action == cli.ACTION_EXEC:
        run_exec(args)
    elif args.action == cli.ACTION_SHOW_CONFIG:
        run_show_config()


def run_exec(args):
    config = get_config(args)
    override_config(args, config)
    setup_logging(config)

    execution = ProcessExecution([args.command] + args.arg)
    job = Job(args.id, execution)
    runner.run(job)


def run_show_config():
    cnf.print_config()


def get_config(args):
    if args.def_config:
        config_file_path = paths.default_config_file_path()
    else:
        config_file_path = paths.lookup_config_file_path()

    return cnf.read_config(config_file_path)


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
        log_file_path = _expand_user(get_attr(config, cnf.LOG_FILE_PATH)) or paths.log_file_path(create=True)
        log.setup_file(file_level, log_file_path)


def _expand_user(file):
    if file is None or not file.startswith('~'):
        return file

    return os.path.expanduser(file)


if __name__ == '__main__':
    main(sys.argv[1:])

# main(['exec', '--log-disabled', 'ls', '-l'])
