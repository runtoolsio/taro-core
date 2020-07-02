import logging
import os
import signal

import sys

from taro import cli, cnf, ps, persistence, http, hostinfo, cmd
from taro.api import Client
from taro.job import DisabledJob
from taro.listening import Receiver, EventPrint, StoppingListener
from taro.util import utc_now
from taro.view import disabled as view_dis
from taro.view import instance as view_inst

logger = logging.getLogger(__name__)


def main_cli():
    main(None)


def main(args):
    args = cli.parse_args(args)
    # cmd.run(args)

    if args.action == cli.ACTION_EXEC:
        cmd.run(args)
    elif args.action == cli.ACTION_PS:
        cmd.run(args)
    elif args.action == cli.ACTION_JOBS:
        cmd.run(args)
    elif args.action == cli.ACTION_RELEASE:
        cmd.run(args)
    elif args.action == cli.ACTION_LISTEN:
        cmd.run(args)
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
    cnf.init(args)
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
    cnf.init(args)
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
    cnf.print_config(args)


def run_hostinfo():
    host_info = hostinfo.read_hostinfo()
    for name, value in host_info.items():
        print(f"{name}: {value}")


if __name__ == '__main__':
    main(sys.argv[1:])
