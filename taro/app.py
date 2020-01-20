import logging
import os
import signal
import sys

from taro import cli, paths, cnf, log, runner
from taro.api import Server, Client
from taro.job import Job
from taro.listening import Dispatcher, Receiver
from taro.process import ProcessExecution
from taro.runner import RunnerJobInstance
from taro.term import Term
from taro.util import get_attr, set_attr

logger = logging.getLogger(__name__)


def main(args):
    args = cli.parse_args(args)

    if args.action == cli.ACTION_EXEC:
        run_exec(args)
    elif args.action == cli.ACTION_PS:
        run_ps(args)
    elif args.action == cli.ACTION_RELEASE:
        run_release(args)
    elif args.action == cli.ACTION_LISTEN:
        run_listen(args)
    elif args.action == cli.ACTION_CONFIG:
        if args.config_action == cli.ACTION_CONFIG_SHOW:
            run_show_config(args)


def run_exec(args):
    config = get_config(args)
    override_config(args, config)
    setup_logging(config)

    all_args = [args.command] + args.arg
    execution = ProcessExecution(all_args)
    job_id = args.id or " ".join(all_args)
    job = Job(job_id, execution, wait=args.wait or '')
    job_instance = RunnerJobInstance(job)
    term = Term(job_instance)
    signal.signal(signal.SIGTERM, term.terminate)
    signal.signal(signal.SIGINT, term.interrupt)
    api = Server(job_instance)
    api_started = api.start()
    if not api_started:
        logger.warning("event=[api_not_started] message=[Interface for managing the job failed to start]")
    dispatcher = Dispatcher()
    runner.register_observer(dispatcher)
    try:
        job_instance.run()
    finally:
        api.stop()
        dispatcher.close()


def run_ps(args):
    client = Client()
    try:
        client.read_job_info()
    finally:
        client.close()


def run_release(args):
    client = Client()
    try:
        client.release_jobs(args.wait)
    finally:
        client.close()


def run_listen(args):
    receiver = Receiver()
    signal.signal(signal.SIGTERM, lambda _, __: receiver.stop())
    signal.signal(signal.SIGINT, lambda _, __: receiver.stop())
    receiver.start()


def run_show_config(args):
    cnf.print_config(get_config_file_path(args))


def get_config(args):
    config_file_path = get_config_file_path(args)
    return cnf.read_config(config_file_path)


def get_config_file_path(args):
    if hasattr(args, 'config') and args.config:
        return _expand_user(args.config)
    elif args.def_config:
        return paths.default_config_file_path()
    else:
        return paths.lookup_config_file_path()


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
    log.init()

    if not get_attr(config, cnf.LOG_ENABLED, default=True):
        log.disable()
        return

    stdout_level = get_attr(config, cnf.LOG_STDOUT_LEVEL, default='off').lower()
    if stdout_level != 'off':
        log.setup_console(stdout_level)

    file_level = get_attr(config, cnf.LOG_FILE_LEVEL, default='off').lower()
    if file_level != 'off':
        log_file_path = _expand_user(get_attr(config, cnf.LOG_FILE_PATH)) or paths.log_file_path(create=True)
        log.setup_file(file_level, log_file_path)


def _expand_user(file):
    if file is None or not file.startswith('~'):
        return file

    return os.path.expanduser(file)


if __name__ == '__main__':
    main(sys.argv[1:])
