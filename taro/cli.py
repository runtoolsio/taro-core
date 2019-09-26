import argparse


def parse_args(args):
    parser = argparse.ArgumentParser(description='Manage your jobs with Taro')
    common = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='action')  # command/action

    exec_parser = subparsers.add_parser('exec', parents=[common], description='Execute command', add_help=False)
    config_group = exec_parser.add_argument_group('config override', 'these options override entries from config file')
    # exec_parser.add_argument('--log', nargs='+', action='append', type=str, metavar='<arg>',
    #                          help='<logger> [level] [arg] ...', default=[])
    # exec_parser.add_argument('--log-level', type=str, default=logging.INFO)  # Remove default
    # exec_parser.add_argument('-t', '--timeout', type=int)
    # # Terms command and arguments taken from python doc and docker run help,
    # # for this app these are operands (alternatively arguments)
    exec_parser.add_argument('--dry-run', action='store_true')  # TODO
    exec_parser.add_argument('--id', type=str, default='anonymous', help='job ID')
    config_group.add_argument('--log-disabled', action='store_true')
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

    _check_log_collision(args, parser)
    return parser.parse_args(args)


def _check_log_collision(args, parser):
    if '--log-disabled' in args:
        for arg in args:
            if arg != '--log-disabled' and arg.startswith('--log-'):
                parser.error("Conflicting options --log-disabled and {}".format(arg))
