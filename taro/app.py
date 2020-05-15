import itertools
import logging
import os
import signal
import sqlite3
import sys

from taro import cli, paths, cnf, runner, ps, jfilter, log, PluginBase, persistence
from taro.api import Server, Client
from taro.cnf import Config
from taro.execution import ExecutionState
from taro.jfilter import AllFilter
from taro.job import Job
from taro.listening import Dispatcher, Receiver, EventPrint, StoppingListener
from taro.process import ProcessExecution
from taro.ps import Column
from taro.runner import RunnerJobInstance
from taro.term import Term
from taro.util import set_attr, expand_user

logger = logging.getLogger(__name__)

USE_MINIMAL_CONFIG = False
EXT_PLUGIN_MODULE_PREFIX = 'taro_'
DEFAULT_PS_COLUMNS = [ps.JOB_ID, ps.INSTANCE_ID, ps.CREATED, ps.EXEC_TIME, ps.STATE, ps.STATUS]


def main_cli():
    main(None)


def main(args):
    args = cli.parse_args(args)

    if args.action == cli.ACTION_EXEC:
        run_exec(args)
    elif args.action == cli.ACTION_PS:
        run_ps(args)
    elif args.action == cli.ACTION_JOB:
        run_job(args)
    elif args.action == cli.ACTION_JOBS:
        run_jobs(args)
    elif args.action == cli.ACTION_RELEASE:
        run_release(args)
    elif args.action == cli.ACTION_LISTEN:
        run_listen(args)
    elif args.action == cli.ACTION_WAIT:
        run_wait(args)
    elif args.action == cli.ACTION_STOP:
        run_stop(args)
    elif args.action == cli.ACTION_CONFIG:
        if args.config_action == cli.ACTION_CONFIG_SHOW:
            run_show_config(args)


def run_exec(args):
    config_ns = get_config(args)
    override_config(args, config_ns)
    config = Config(config_ns)
    log.setup(config)

    all_args = [args.command] + args.arg
    execution = ProcessExecution(all_args, args.read_output)
    job_id = args.id or " ".join(all_args)
    job = Job(job_id, pending=args.pending or '')
    job_instance = RunnerJobInstance(job, execution)
    term = Term(job_instance)
    signal.signal(signal.SIGTERM, term.terminate)
    signal.signal(signal.SIGINT, term.interrupt)
    api = Server(job_instance)
    api_started = api.start()
    if not api_started:
        logger.warning("event=[api_not_started] message=[Interface for managing the job failed to start]")
    db_con = None
    if config.persistence_enabled:
        db_con = init_sqlite(config.persistence_database)
    else:
        persistence.disable()
    dispatcher = Dispatcher()
    runner.register_observer(dispatcher)
    for plugin in PluginBase.create_plugins(EXT_PLUGIN_MODULE_PREFIX, config.plugins).values():
        try:
            plugin.new_job_instance(job_instance)
        except BaseException as e:
            logger.warning("event=[plugin_failed] reason=[exception_on_new_job_instance] detail=[%s]", e)
    try:
        job_instance.run()
    finally:
        api.stop()
        dispatcher.close()
        if db_con:
            db_con.close()


def init_sqlite(database_path):
    db_con = sqlite3.connect(database_path or str(paths.sqlite_db_path(True)))
    persistence.init_sqlite(db_con)
    return db_con


def run_ps(args):
    client = Client()
    try:
        jobs = client.read_jobs_info()
        ps.print_table(jobs, DEFAULT_PS_COLUMNS, show_header=True, pager=False)
    finally:
        client.close()


def run_job(args):
    config = Config(get_config(args))
    if args.command == 'disable':
        if not config.persistence_enabled:
            print('Persistence is disabled. Enable persistence in config file to be able to store disabled jobs',
                  file=sys.stderr)
            exit(1)
        db_con = init_sqlite(config.persistence_database)
        jobs = args.arg
        try:
            persistence.add_disabled_jobs(jobs)
            print("Jobs disabled: {}".format(",".join(jobs)))
        finally:
            if db_con:
                db_con.close()
    elif args.command == 'list-disabled':
        if config.persistence_enabled:
            db_con = init_sqlite(config.persistence_database)
            try:
                disabled_jobs = persistence.read_disabled_jobs()
                JOB_ID = Column('JOB ID', 30, lambda i: i)
                ps.print_table(disabled_jobs, [JOB_ID], show_header=True, pager=False)
            finally:
                if db_con:
                    db_con.close()
        else:
            print("Persistence is disabled")


def run_jobs(args):
    jobs = []

    client = Client()
    try:
        jobs += client.read_jobs_info()
    finally:
        client.close()

    db_con = sqlite3.connect(str(paths.sqlite_db_path(True)))
    try:
        persistence.init_sqlite(db_con)
        jobs += persistence.read_jobs(chronological=args.chronological)
    finally:
        db_con.close()

    columns = [ps.JOB_ID, ps.INSTANCE_ID, ps.CREATED, ps.ENDED, ps.EXEC_TIME, ps.STATE, ps.STATUS]
    sorted_jobs = sorted(jobs, key=lambda j: j.lifecycle.changed(ExecutionState.CREATED),
                         reverse=not args.chronological)
    job_filter = _build_job_filter(args)
    filtered_jobs = filter(job_filter, sorted_jobs)
    limited_jobs = itertools.islice(filtered_jobs, 0, args.lines or None)
    ps.print_table(limited_jobs, columns, show_header=True, pager=not args.no_pager)


def _build_job_filter(args):
    job_filter = AllFilter()
    if args.id:
        job_filter <<= jfilter.create_id_filter(args.id)
    if args.finished:
        job_filter <<= jfilter.finished_filter
    if args.today:
        job_filter <<= jfilter.today_filter
    if args.since:
        job_filter <<= jfilter.create_since_filter(args.since)
    if args.until:
        job_filter <<= jfilter.create_until_filter(args.until)

    return job_filter


def run_release(args):
    client = Client()
    try:
        client.release_jobs(args.pending)
    finally:
        client.close()


def run_listen(args):
    receiver = Receiver()
    receiver.listeners.append(EventPrint())
    signal.signal(signal.SIGTERM, lambda _, __: receiver.stop())
    signal.signal(signal.SIGINT, lambda _, __: receiver.stop())
    receiver.start()


def run_wait(args):
    def condition(job_info): return not args.states or job_info.state.name in args.states

    receiver = Receiver()
    receiver.listeners.append(EventPrint(condition))
    receiver.listeners.append(StoppingListener(receiver, condition, args.count))
    signal.signal(signal.SIGTERM, lambda _, __: stop_server_and_exit(receiver, signal.SIGTERM))
    signal.signal(signal.SIGINT, lambda _, __: stop_server_and_exit(receiver, signal.SIGINT))
    receiver.start()


def run_stop(args):
    client = Client()
    try:
        all_jobs = client.read_jobs_info()
        jobs = [job for job in all_jobs if job.job_id == args.job or job.instance_id == args.job]
        if len(jobs) > 1 and not args.all:
            print('No action performed, because the criteria matches more than one job.'
                  'Use --all flag if you wish to stop them all:' + os.linesep)
            columns = [ps.JOB_ID, ps.INSTANCE_ID, ps.CREATED, ps.EXEC_TIME, ps.STATE, ps.STATUS]
            ps.print_table(jobs, columns, show_header=True, pager=False)
            return  # Exit code non-zero?

        inst_results = client.stop_jobs([job.instance_id for job in jobs], args.interrupt)
        for i_res in inst_results:
            print(f"{i_res[0].job_id}@{i_res[0].instance_id} -> {i_res[1]}")
    finally:
        client.close()


def stop_server_and_exit(server, signal_number: int):
    server.stop()
    sys.exit(128 + signal_number)


def run_show_config(args):
    cnf.print_config(get_config_file_path(args))


def get_config(args):
    config_file_path = get_config_file_path(args)
    return cnf.read_config(config_file_path)


def get_config_file_path(args):
    if hasattr(args, 'config') and args.config:
        return expand_user(args.config)
    if hasattr(args, 'def_config') and args.def_config:
        return paths.default_config_file_path()
    # Keep following condition as the last one so USE_MINIMAL_CONFIG can be overridden by args
    if (hasattr(args, 'min_config') and args.min_config) or USE_MINIMAL_CONFIG:
        return paths.minimal_config_file_path()

    return paths.lookup_config_file()


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


if __name__ == '__main__':
    main(sys.argv[1:])
