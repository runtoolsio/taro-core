import argparse
import re
from argparse import RawTextHelpFormatter
from datetime import datetime

from taro.jobs.execution import ExecutionState
from taro.jobs.persistence import SortCriteria
from taroapp import version

ACTION_EXEC = 'exec'
ACTION_PS = 'ps'
ACTION_HISTORY = 'history'
ACTION_HISTORYREMOVE = 'history-remove'
ACTION_RELEASE = 'release'
ACTION_LISTEN = 'listen'
ACTION_WAIT = 'wait'
ACTION_STOP = 'stop'
ACTION_TAIL = 'tail'
ACTION_DISABLE = 'disable'
ACTION_LIST_DISABLED = 'list-disabled'
ACTION_ENABLE = 'enable'
ACTION_CONFIG = 'config'
ACTION_CONFIG_SHOW = 'show'
ACTION_CONFIG_RESET = 'reset'
ACTION_HOSTINFO = 'hostinfo'


def parse_args(args):
    # TODO destination required
    parser = argparse.ArgumentParser(description='Manage your jobs with Taro')
    parser.add_argument("-V", "--version", action='version', help="Show version and exit.", version=version.get())
    common = argparse.ArgumentParser()  # parent parser for subparsers in case they need to share common options
    common.add_argument('--set', type=str, action='append', help='override value of configuration field')
    subparsers = parser.add_subparsers(dest='action')  # command/action

    _init_exec_parser(common, subparsers)
    _init_ps_parser(common, subparsers)
    _init_history_parser(common, subparsers)
    _init_release_parser(common, subparsers)
    _init_listen_parser(common, subparsers)
    _init_wait_parser(common, subparsers)
    _init_stop_parser(common, subparsers)
    _init_tail_parser(common, subparsers)
    _init_disable_parser(common, subparsers)
    _init_list_disabled_parser(common, subparsers)
    _init_enable_parser(common, subparsers)
    _init_config_parser(subparsers)
    _init_hostinfo_parser(common, subparsers)
    _init_history_remove_parser(common, subparsers)

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
    exec_parser.add_argument('--warn-time', type=_warn_time_type, action='append', default=[],
                             help='Time value for execution time exceeded warning')
    exec_parser.add_argument('--warn-output', type=str, action='append', default=[],
                             help='Regex value for stdout warning')

    exec_parser.add_argument('--dry-run', type=_str2state, nargs='?', const=ExecutionState.COMPLETED,
                             help='executing without actual running of the command - optional termination state arg')

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
    ps_parser.add_argument('instance', nargs='?', default=None, type=str, help='instance filter')
    ps_parser.add_argument('-f', '--format', type=str, choices=['table', 'json', 'jsonp'], default='table',
                           help='output format')


def _init_history_parser(common, subparsers):
    """
    Creates parsers for `history` command

    :param common: parent parser
    :param subparsers: sub-parser for history parser to be added to
    """

    hist_parser = subparsers.add_parser(
        ACTION_HISTORY, aliases=['hist'], parents=[common], description='Show jobs history', add_help=False)

    filter_group = hist_parser.add_argument_group('filtering', 'These options allows to filter returned jobs')
    filter_group.add_argument('id', nargs='?', type=str,
                              help='Job or instance ID matching pattern for result filtering')
    filter_group.add_argument('-T', '--today', action='store_true', help='Return only jobs created today (local)')
    filter_group.add_argument('-S', '--since', type=_str2dt, help='Show entries not older than the specified date')
    filter_group.add_argument('-U', '--until', type=_str2dt, help='Show entries not newer than the specified date')
    filter_group.add_argument('-n', '--lines', type=int, help='Number of history entries to show')
    filter_group.add_argument('-L', '--last', action='store_true', help='Show last execution of each job')

    hist_parser.add_argument('-C', '--config', type=str, help='path to custom config file')
    hist_parser.add_argument('-dc', '--def-config', action='store_true', help='ignore config files and use defaults')
    hist_parser.add_argument('-a', '--asc', '--ascending', action='store_true', help='Ascending sort')
    hist_parser.add_argument('-s', '--sort', type=str, choices=[s.name.lower() for s in SortCriteria],
                             default=SortCriteria.CREATED.name.lower(), help='Sorting criteria')
    hist_parser.add_argument('-P', '--no-pager', action='store_true', help='Do not use pager for output')


def _init_history_remove_parser(common, subparsers):
    """
    Creates parsers for `history-remove` command

    :param common: parent parser
    :param subparsers: sub-parser for history-remove parser to be added to
    """

    hist_rm_parser = subparsers.add_parser(
        ACTION_HISTORYREMOVE, parents=[common], description="Remove job from history", add_help=False)

    hist_rm_parser.add_argument('id', nargs=argparse.REMAINDER, type=str, help='Job or instance ID')


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
    release_parser.add_argument('instance', nargs='?', default=None, type=str, help='instance filter')


def _init_wait_parser(common, subparsers):
    """). The special characters used in shell-style wildcards are:
    Creates parsers for `wait` command

    :param common: parent parser
    :param subparsers: sub-parser for wait parser to be added to
    """

    wait_parser = subparsers.add_parser(ACTION_WAIT, parents=[common], description='Wait for job state', add_help=False)
    wait_parser.add_argument('instance', nargs='?', default=None, type=str, help='instance filter')
    wait_parser.add_argument('-c', '--count', type=int, default=1, help='Number of occurrences to finish the wait')
    wait_parser.add_argument('-s', '--states', type=_str2state, metavar='STATES', nargs=argparse.REMAINDER,
                             help='States for which the command waits')


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
    stop_parser.add_argument('instance', type=str, metavar='INSTANCE', help='Instance to stop')


def _init_tail_parser(common, subparsers):
    """
    Creates parsers for `tail` command

    :param common: parent parser
    :param subparsers: sub-parser for tail parser to be added to
    """

    tail_parser = subparsers.add_parser(ACTION_TAIL, parents=[common], description='Print last output', add_help=False)
    tail_parser.add_argument('instance', nargs='?', default=None, type=str, help='instance filter')
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


def _init_enable_parser(common, subparsers):
    """
    Creates parsers for `enable` command

    :param common: parent parser
    :param subparsers: sub-parser for enable parser to be added to
    """

    enable_parser = subparsers.add_parser(
        ACTION_ENABLE, parents=[common], description='Enable jobs (persistence required)', add_help=False)

    enable_parser.add_argument('-C', '--config', type=str, help='path to custom config file')
    enable_parser.add_argument('-dc', '--def-config', action='store_true', help='ignore config files and use defaults')

    enable_parser.add_argument('jobs', type=str, metavar='JOB', nargs=argparse.REMAINDER, help="job IDs to enable")


def _init_config_parser(subparsers):
    """
    Creates parsers for `config` command

    :param subparsers: sub-parser for config parser to be added to
    """

    config_parser = subparsers.add_parser(
        ACTION_CONFIG, description='Config related actions', add_help=True)

    # TODO Add required=True if migrated to >=3.7
    config_subparsers = config_parser.add_subparsers(dest='config_action')

    show_config_parser = config_subparsers.add_parser(
        ACTION_CONFIG_SHOW, description='Print config used by exec command or config specified by an option',
        add_help=False)
    show_config_parser.add_argument('-dc', '--def-config', action='store_true', help='show default config')

    reset_config_parser = config_subparsers.add_parser(ACTION_CONFIG_RESET, description='reset config file',
                                                         add_help=False)


def _init_hostinfo_parser(common, subparsers):
    """
    Creates parsers for `hostinfo` command

    :param common: parent parser
    :param subparsers: sub-parser for hostinfo parser to be added to
    """

    hostinfo_parser = subparsers.add_parser(
        ACTION_HOSTINFO, parents=[common], description='Show host info', add_help=False)


def _str2dt(v):
    try:
        return datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(v, "%Y-%m-%d %H:%M")
        except ValueError:
            return datetime.strptime(v, "%Y-%m-%d")


# TODO Consider: change to str (like SortCriteria case) and remove this function
def _str2state(v):
    try:
        return ExecutionState[v.upper()]
    except KeyError:
        raise argparse.ArgumentTypeError('Arguments can be only valid execution states: '
                                         + ", ".join([e.name.lower() for e in ExecutionState]))


def _warn_time_type(arg_value):
    regex = r'^\d+[smhd]$'
    pattern = re.compile(regex)
    if not pattern.match(arg_value):
        raise argparse.ArgumentTypeError(f"Execution time warning value {arg_value} does not match pattern {regex}")
    return arg_value


def _build_warn_validation_regex(*warn_regex):
    return "^(" + "|".join(warn_regex) + ")$"


def _check_collisions(parser, parsed):
    """
    Check that incompatible combinations of options were not used

    :param parser: parser
    :param parsed: parsed arguments
    """
    config_options = []
    if hasattr(parsed, 'def_config') and parsed.def_config:
        config_options.append('def_config')
    if hasattr(parsed, 'min_config') and parsed.min_config:
        config_options.append('min_config')
    if hasattr(parsed, 'config') and parsed.config:
        config_options.append('config')

    if len(config_options) > 1:
        parser.error('Conflicting options: ' + str(config_options))
