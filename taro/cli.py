import argparse
import re
from argparse import RawTextHelpFormatter
from datetime import datetime

from taro import cnf, warning

ACTION_EXEC = 'exec'
ACTION_PS = 'ps'
ACTION_JOB = 'job'
ACTION_JOBS = 'jobs'
ACTION_RELEASE = 'release'
ACTION_LISTEN = 'listen'
ACTION_WAIT = 'wait'
ACTION_STOP = 'stop'
ACTION_TAIL = 'tail'
ACTION_DISABLE = 'disable'
ACTION_LIST_DISABLED = 'list-disabled'
ACTION_HTTP = 'http'
ACTION_CONFIG = 'config'
ACTION_CONFIG_SHOW = 'show'
ACTION_HOSTINFO = 'hostinfo'

_true_options = ['yes', 'true', 't', 'y', '1', 'on']
_false_options = ['no', 'false', 'f', 'n', '0', 'off']
_all_boolean_options = _true_options + _false_options

_log_levels = ['critical', 'fatal', 'error', 'warn', 'warning', 'info', 'debug', 'off']

_job_commands = ['enable', 'disable', 'list-disabled']


def parse_args(args):
    # TODO destination required
    parser = argparse.ArgumentParser(description='Manage your jobs with Taro')
    common = argparse.ArgumentParser()  # parent parser for subparsers in case they need to share common options
    subparsers = parser.add_subparsers(dest='action')  # command/action

    _init_exec_parser(common, subparsers)
    _init_ps_parser(common, subparsers)
    _init_job_parser(common, subparsers)
    _init_jobs_parser(common, subparsers)
    _init_release_parser(common, subparsers)
    _init_listen_parser(common, subparsers)
    _init_wait_parser(common, subparsers)
    _init_stop_parser(common, subparsers)
    _init_tail_parser(common, subparsers)
    _init_disable_parser(common, subparsers)
    _init_list_disabled_parser(common, subparsers)
    _init_http_parser(common, subparsers)
    _init_show_config_parser(common, subparsers)
    _init_hostinfo_parser(common, subparsers)

    parsed = parser.parse_args(args)
    _check_collisions(parser, parsed)
    return parsed


def _init_exec_parser(common, subparsers):
    """
    Creates parser for `exec` command

    :param common: parent parser
    :param subparsers: sub-parser for exec parser to be added to
    """

    exec_parser = subparsers.add_parser(
        ACTION_EXEC, formatter_class=RawTextHelpFormatter, parents=[common], description='Execute command',
        add_help=False)

    # General options
    exec_parser.add_argument('-dc', '--def-config', action='store_true', help='ignore config files and use defaults')
    exec_parser.add_argument('-mc', '--min-config', action='store_true',
                             help='ignore config files and use minimum configuration')
    exec_parser.add_argument('-C', '--config', type=str, help='path to custom config file')
    exec_parser.add_argument('--id', type=str, help='defines job ID')
    exec_parser.add_argument('-b', '--bypass-output', action='store_true', help='output is not piped')
    exec_parser.add_argument('-o', '--no-overlap', action='store_true', default=False,
                             help='skip if job with the same ID is already running')
    # TODO delay
    # exec_parser.add_argument('-t', '--timeout', type=int) TODO implement
    exec_parser.add_argument('-p', '--pending', type=str, help='specifies pending value for releasing of this job')
    # exec_parser.add_argument('-w', '--wait', type=str, help='execution will wait for other jobs') TODO implement
    exec_parser.add_argument('-W', '--warn', type=_warn_type, action='append', help='Add warning check')

    # Config override options
    config_group = exec_parser.add_argument_group('config override', 'these options override entries from config file')
    config_group.add_argument('--log-enabled', type=_str2bool, metavar="{{{}}}".format(','.join(_all_boolean_options)),
                              help='overrides ' + cnf.LOG_ENABLED)
    config_group.add_argument('--log-stdout', type=str, choices=_log_levels,
                              help='overrides ' + cnf.LOG_STDOUT_LEVEL)
    config_group.add_argument('--log-file', type=str, choices=_log_levels,
                              help='overrides ' + cnf.LOG_FILE_LEVEL)
    config_group.add_argument('--log-file-path', type=str, metavar='PATH',
                              help='overrides ' + cnf.LOG_FILE_PATH)

    # Terms command and arguments taken from python doc and docker run help,
    # for this app (or rather exec command) these are operands (alternatively arguments)
    exec_parser.add_argument('command', type=str, metavar='COMMAND', help='program to execute')
    exec_parser.add_argument('arg', type=str, metavar='ARG', nargs=argparse.REMAINDER, help="program arguments")


def _init_ps_parser(common, subparsers):
    """
    Creates parsers for `ps` command

    :param common: parent parser
    :param subparsers: sub-parser for ps parser to be added to
    """

    ps_parser = subparsers.add_parser(ACTION_PS, parents=[common], description='Show running jobs', add_help=False)


def _init_job_parser(common, subparsers):
    """
    Creates parsers for `job` command

    :param common: parent parser
    :param subparsers: sub-parser for job parser to be added to
    """

    job_parser = subparsers.add_parser(
        ACTION_JOB, parents=[common], description='Configure jobs', add_help=False)

    job_parser.add_argument('-C', '--config', type=str, help='path to custom config file')
    job_parser.add_argument('-dc', '--def-config', action='store_true', help='ignore config files and use defaults')

    job_parser.add_argument('command', type=str, metavar='COMMAND', choices=_job_commands, help='command to execute')
    job_parser.add_argument('arg', type=str, metavar='ARG', nargs=argparse.REMAINDER, help="command arguments")


def _init_jobs_parser(common, subparsers):
    """
    Creates parsers for `jobs` command

    :param common: parent parser
    :param subparsers: sub-parser for jobs parser to be added to
    """

    jobs_parser = subparsers.add_parser(
        ACTION_JOBS, parents=[common], description='Show jobs', add_help=False)

    filter_group = jobs_parser.add_argument_group('filtering', 'These options allows to filter returned jobs')
    filter_group.add_argument('--id', type=str, help='Job or instance ID matching pattern for result filtering')
    filter_group.add_argument('-F', '--finished', action='store_true', help='Return only finished jobs')
    filter_group.add_argument('-T', '--today', action='store_true', help='Return only jobs created today (local)')
    filter_group.add_argument('-S', '--since', type=_str2dt, help='Show entries not older than the specified date')
    filter_group.add_argument('-U', '--until', type=_str2dt, help='Show entries not newer than the specified date')

    jobs_parser.add_argument('-C', '--config', type=str, help='path to custom config file')
    jobs_parser.add_argument('-dc', '--def-config', action='store_true', help='ignore config files and use defaults')
    jobs_parser.add_argument('-n', '--lines', type=int, help='Number of job entries to show')
    jobs_parser.add_argument('-c', '--chronological', action='store_true', help='Display jobs in chronological order')
    jobs_parser.add_argument('-P', '--no-pager', action='store_true', help='Do not use pager for output')


def _init_release_parser(common, subparsers):
    """
    Creates parsers for `release` command

    :param common: parent parser
    :param subparsers: sub-parser for release parser to be added to
    """

    release_parser = subparsers.add_parser(ACTION_RELEASE, parents=[common],
                                           description='Release jobs in pending state', add_help=False)
    release_parser.add_argument('pending', type=str, metavar='WAIT', help='Pending condition value')


def _init_listen_parser(common, subparsers):
    """
    Creates parsers for `listen` command

    :param common: parent parser
    :param subparsers: sub-parser for listen parser to be added to
    """

    release_parser = subparsers.add_parser(ACTION_LISTEN, parents=[common],
                                           description='Print job state changes', add_help=False)


def _init_wait_parser(common, subparsers):
    """
    Creates parsers for `wait` command

    :param common: parent parser
    :param subparsers: sub-parser for wait parser to be added to
    """

    wait_parser = subparsers.add_parser(ACTION_WAIT, parents=[common], description='Wait for job state', add_help=False)
    wait_parser.add_argument('-c', '--count', type=int, default=1, help='Number of occurrences to finish the wait')
    wait_parser.add_argument('states', type=str, metavar='STATES', nargs=argparse.REMAINDER,
                             help='States or group of states for which the command waits')


def _init_stop_parser(common, subparsers):
    """
    Creates parsers for `stop` command

    :param common: parent parser
    :param subparsers: sub-parser for stop parser to be added to
    """

    stop_parser = subparsers.add_parser(ACTION_STOP, parents=[common], description='Stop job', add_help=False)
    stop_parser.add_argument('-I', '--interrupt', action='store_true',
                             help='Set final state to INTERRUPTED which is an error state')
    stop_parser.add_argument('--all', action='store_true', help='Force stop all if there are more jobs to stop')
    stop_parser.add_argument('job', type=str, metavar='JOB', help='ID of the job to stop')


def _init_tail_parser(common, subparsers):
    """
    Creates parsers for `tail` command

    :param common: parent parser
    :param subparsers: sub-parser for tail parser to be added to
    """

    tail_parser = subparsers.add_parser(ACTION_TAIL, parents=[common], description='Print last output', add_help=False)
    tail_parser.add_argument('-f', '--follow', action='store_true', help='Keep printing')


def _init_disable_parser(common, subparsers):
    """
    Creates parsers for `disable` command

    :param common: parent parser
    :param subparsers: sub-parser for disable parser to be added to
    """

    disable_parser = subparsers.add_parser(
        ACTION_DISABLE, parents=[common], description='Disable jobs (persistence required)', add_help=False)

    disable_parser.add_argument('-C', '--config', type=str, help='path to custom config file')
    disable_parser.add_argument('-dc', '--def-config', action='store_true', help='ignore config files and use defaults')
    disable_parser.add_argument('-regex', action='store_true', help='job ID is regular expression')

    disable_parser.add_argument('jobs', type=str, metavar='JOB', nargs=argparse.REMAINDER, help="job IDs to disable")


def _init_list_disabled_parser(common, subparsers):
    """
    Creates parsers for `list-disabled` command

    :param common: parent parser
    :param subparsers: sub-parser for list-disabled parser to be added to
    """

    ld_parser = subparsers.add_parser(
        ACTION_LIST_DISABLED, parents=[common], description='List disabled jobs (persistence required)', add_help=False)

    ld_parser.add_argument('-C', '--config', type=str, help='path to custom config file')
    ld_parser.add_argument('-dc', '--def-config', action='store_true', help='ignore config files and use defaults')


def _init_http_parser(common, subparsers):
    """
    Creates parsers for `http` command

    :param common: parent parser
    :param subparsers: sub-parser for http parser to be added to
    """

    ld_parser = subparsers.add_parser(
        ACTION_HTTP, parents=[common], description='Execute job controlled by HTTP interface', add_help=False)

    ld_parser.add_argument('--url', required=True, type=str, help='URL for triggering the job')
    ld_parser.add_argument('-D', '--data', type=str, help='Request body for the job trigger')
    ld_parser.add_argument('-M', '--monitor-url', type=str, help='URL for monitoring of the job')
    ld_parser.add_argument('-R', '--is-running', type=str, help='Condition to find out if the job is running')
    ld_parser.add_argument('-S', '--status', type=str, help='TODO')


def _init_show_config_parser(common, subparsers):
    """
    Creates parsers for `config` command

    :param common: parent parser
    :param subparsers: sub-parser for config parser to be added to
    """

    config_parser = subparsers.add_parser(
        ACTION_CONFIG, parents=[common], description='Config related actions', add_help=False)
    config_subparsers = config_parser.add_subparsers(dest='config_action')
    show_config_parser = config_subparsers.add_parser(
        ACTION_CONFIG_SHOW, parents=[common],
        description='Print config used by exec command or config specified by an option', add_help=False)
    show_config_parser.add_argument('-dc', '--def-config', action='store_true', help='show default config')
    show_config_parser.add_argument('-mc', '--min-config', action='store_true', help='show minimum config')


def _init_hostinfo_parser(common, subparsers):
    """
    Creates parsers for `hostinfo` command

    :param common: parent parser
    :param subparsers: sub-parser for hostinfo parser to be added to
    """

    hostinfo_parser = subparsers.add_parser(
        ACTION_HOSTINFO, parents=[common], description='Show host info', add_help=False)


# Maxim's solution: https://stackoverflow.com/questions/15008758
def _str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in _true_options:
        return True
    elif v.lower() in _false_options:
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


def _str2dt(v):
    try:
        return datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(v, "%Y-%m-%d %H:%M")
        except ValueError:
            return datetime.strptime(v, "%Y-%m-%d")


def _warn_type(arg_value):
    p = r"^(" + warning.EXEC_TIME_WARN_REGEX + "|" + warning.FILE_CONTAINS_REGEX + r"|free_disk_space:.+<\d+[KMGT]B)$"
    pattern = re.compile(p)
    if not pattern.match(arg_value.replace(" ", "").rstrip()):
        raise argparse.ArgumentTypeError(f"Warning value {arg_value} does not match pattern {p}")
    return arg_value


def _check_collisions(parser, parsed):
    """
    Check that incompatible combinations of options were not used

    :param parser: parser
    :param parsed: parsed arguments
    """
    if hasattr(parsed, 'log_enabled') and parsed.log_enabled is not None and not parsed.log_enabled:
        for arg, val in vars(parsed).items():
            if arg != 'log_enabled' and arg.startswith('log_') and val is not None:
                parser.error("Conflicting options: log-enabled is set to false but {} is specified".format(arg))

    config_options = []
    if hasattr(parsed, 'def_config') and parsed.def_config:
        config_options.append('def_config')
    if hasattr(parsed, 'min_config') and parsed.min_config:
        config_options.append('min_config')
    if hasattr(parsed, 'config') and parsed.config:
        config_options.append('config')

    if len(config_options) > 1:
        parser.error('Conflicting options: ' + str(config_options))
