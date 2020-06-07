import itertools
import logging
import os
import signal
import sys

from taro import cli, paths, cnf, runner, ps, jfilter, log, PluginBase, persistence, http, hostinfo
from taro.api import Server, Client
from taro.cnf import Config
from taro.execution import ExecutionState
from taro.jfilter import AllFilter
from taro.job import DisabledJob
from taro.listening import Dispatcher, Receiver, EventPrint, StoppingListener
from taro.process import ProcessExecution
from taro.runner import RunnerJobInstance
from taro.term import Term
from taro.util import set_attr, expand_user, utc_now
from taro.view import disabled as view_dis
from taro.view import instance as view_inst

logger = logging.getLogger(__name__)

EXT_PLUGIN_MODULE_PREFIX = 'taro_'


def main_cli():
    main(None)


def main(args):
    args = cli.parse_args(args)

    if args.action == cli.ACTION_EXEC:
        run_exec(args)
    elif args.action == cli.ACTION_PS:
        run_ps(args)
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
    elif args.action == cli.ACTION_DISABLE:
        run_disable(args)
    elif args.action == cli.ACTION_LIST_DISABLED:
        run_list_disabled(args)
    elif args.action == cli.ACTION_HTTP:
        run_http(args)
    elif args.action == cli.ACTION_CONFIG:
        if args.config_action == cli.ACTION_CONFIG_SHOW:
            run_show_config(args)
    elif args.action == cli.ACTION_HOSTINFO:
        run_hostinfo()


def setup_config(args):
    config_ns = get_config(args)
    override_config(args, config_ns)
    config = Config(config_ns)
    cnf.config = config
    return config


def run_exec(args):
    config = setup_config(args)
    log.init()
    persistence.init()

    all_args = [args.command] + args.arg
    execution = ProcessExecution(all_args, read_output=not args.bypass_output)
    job_id = args.id or " ".join(all_args)
    job_instance = RunnerJobInstance(job_id, execution)
    term = Term(job_instance)
    signal.signal(signal.SIGTERM, term.terminate)
    signal.signal(signal.SIGINT, term.interrupt)
    if args.pending:
        pending_latch = PendingLatch(args.pending, job_instance.create_latch(ExecutionState.PENDING))
    else:
        pending_latch = None
    api = Server(job_instance, pending_latch)
    api_started = api.start()
    if not api_started:
        logger.warning("event=[api_not_started] message=[Interface for managing the job failed to start]")
    dispatcher = Dispatcher()
    runner.register_state_observer(dispatcher)
    for plugin in PluginBase.create_plugins(EXT_PLUGIN_MODULE_PREFIX, config.plugins).values():  # TODO to plugin module
        try:
            plugin.new_job_instance(job_instance)
        except BaseException as e:
            logger.warning("event=[plugin_failed] reason=[exception_on_new_job_instance] detail=[%s]", e)
    try:
        job_instance.run()
    finally:
        api.stop()
        dispatcher.close()
        persistence.close()


class PendingLatch:

    def __init__(self, value, latch):
        self.value = value
        self.latch = latch

    def release(self, value):
        if self.value == value:
            self.latch()
            return True
        else:
            return False


def run_ps(args):
    client = Client()
    try:
        jobs = client.read_jobs_info()
        ps.print_table(jobs, view_inst.DEFAULT_COLUMNS, show_header=True, pager=False)
    finally:
        client.close()


def run_jobs(args):
    setup_config(args)

    jobs = []

    client = Client()
    try:
        jobs += client.read_jobs_info()
    finally:
        client.close()

    persistence.init()
    try:
        jobs += persistence.read_jobs(chronological=args.chronological)
    finally:
        persistence.close()

    columns = [view_inst.JOB_ID, view_inst.INSTANCE_ID, view_inst.CREATED, view_inst.ENDED, view_inst.EXEC_TIME,
               view_inst.STATE, view_inst.STATUS]
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
            ps.print_table(jobs, view_inst.DEFAULT_COLUMNS, show_header=True, pager=False)
            return  # Exit code non-zero?

        inst_results = client.stop_jobs([job.instance_id for job in jobs], args.interrupt)
        for i_res in inst_results:
            print(f"{i_res[0].job_id}@{i_res[0].instance_id} -> {i_res[1]}")
    finally:
        client.close()


def stop_server_and_exit(server, signal_number: int):
    server.stop()
    sys.exit(128 + signal_number)


def run_disable(args):
    setup_config(args)
    persistence_enabled = persistence.init()

    if not persistence_enabled:
        print('Persistence is disabled. Enable persistence in config file to be able to store disabled jobs',
              file=sys.stderr)
        exit(1)

    jobs = args.jobs
    disabled_jobs = [DisabledJob(j, args.regex, utc_now(), None) for j in args.jobs]
    try:
        persistence.add_disabled_jobs(disabled_jobs)
        print("Jobs disabled: {}".format(",".join(jobs)))
    finally:
        persistence.close()


def run_list_disabled(args):
    setup_config(args)
    persistence_enabled = persistence.init()
    if not persistence_enabled:
        print("Persistence is disabled")
        exit(1)

    try:
        disabled_jobs = persistence.read_disabled_jobs()
        ps.print_table(disabled_jobs, view_dis.DEFAULT_COLUMNS, show_header=True, pager=False)
    finally:
        persistence.close()


def run_http(args):
    http.run(args.url, args.data, args.monitor_url, args.is_running, args.status)


def run_show_config(args):
    cnf.print_config(get_config_file_path(args))


def run_hostinfo():
    host_info = hostinfo.read_hostinfo()
    for name, value in host_info.items():
        print(f"{name}: {value}")


def get_config(args):
    config_file_path = get_config_file_path(args)
    return cnf.read_config(config_file_path)


def get_config_file_path(args):
    if hasattr(args, 'config') and args.config:
        return expand_user(args.config)
    if hasattr(args, 'def_config') and args.def_config:
        return paths.default_config_file_path()
    if hasattr(args, 'min_config') and args.min_config:
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
        if not hasattr(args, arg):
            continue
        arg_value = getattr(args, arg)
        if arg_value is not None:
            set_attr(config, conf.split('.'), arg_value)


if __name__ == '__main__':
    main(sys.argv[1:])
