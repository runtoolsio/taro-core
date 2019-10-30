import argparse
from argparse import RawTextHelpFormatter

_true_options = ['yes', 'true', 't', 'y', '1', 'on']
_false_options = ['no', 'false', 'f', 'n', '0', 'off']


def parse_args(args):
    parser = argparse.ArgumentParser(description='Manage your jobs with Taro')
    common = argparse.ArgumentParser()  # parent parser for subparsers in case they need to share common options
    subparsers = parser.add_subparsers(dest='action')  # command/action

    init_exec_parser(common, subparsers)

    parsed = parser.parse_args(args)
    _check_log_collision(parser, parsed)
    return parsed


def init_exec_parser(common, subparsers):
    """
    Creates parser for exec command

    :param common: parent parser
    :param subparsers: sub-parser for exec parser to be added to
    """

    exec_parser = subparsers.add_parser(
        'exec', formatter_class=RawTextHelpFormatter, parents=[common], description='Execute command', add_help=False)
    config_group = exec_parser.add_argument_group('config override', 'these options override entries from config file')
    exec_parser.add_argument('--dry-run', action='store_true')  # TODO
    exec_parser.add_argument('--id', type=str, default='anonymous', help='job ID')
    # exec_parser.add_argument('-t', '--timeout', type=int)
    config_group.add_argument(
        '--log-enabled',
        type=_str2bool,
        metavar='enabled',
        help='overrides log.enabled, allowed values True={}, False={}'.format(_true_options, _false_options))
    config_group.add_argument('--log-file', type=str, metavar='<log-level>',
                              help='log into {log-file-path} file with given <log-level>')
    config_group.add_argument('--log-file-path', type=str, metavar='<path>',
                              help='log file path')
    config_group.add_argument('--log-stdout', type=str, metavar='<log-level>',
                              help='log into standard output with given <log-level>')
    # Terms command and arguments taken from python doc and docker run help,
    # for this app (or rather exec command) these are operands (alternatively arguments)
    exec_parser.add_argument('command', type=str, metavar='COMMAND', help='program to execute')
    exec_parser.add_argument('arg', type=str, metavar='ARG', nargs=argparse.REMAINDER, help="program arguments")


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


def _check_log_collision(parser, parsed):
    if parsed.log_enabled is not None and not parsed.log_enabled:
        for arg, val in vars(parsed).items():
            if arg != 'log_enabled' and arg.startswith('log_') and val is not None:
                parser.error("Conflicting options: log_enabled is set to false but {} is specified".format(arg))
