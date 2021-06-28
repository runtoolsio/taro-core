import argparse

ACTION_CONFIG = 'config'
ACTION_CONFIG_SHOW = 'show'
ACTION_CONFIG_CREATE = 'create'
ACTION_START = 'start'


def parse_args(args):
    # TODO destination required
    parser = argparse.ArgumentParser(description='Taro Server')
    subparsers = parser.add_subparsers(dest='action')  # command/action

    _init_parser_config(subparsers)
    _init_parser_start(subparsers)

    return parser.parse_args(args)


def _init_parser_config(subparsers):
    """
    Creates parsers for `config` command

    :param subparsers: sub-parser for config parser to be added to
    """

    config_parser = subparsers.add_parser(ACTION_CONFIG, description='Config related actions', add_help=False)
    config_subparsers = config_parser.add_subparsers(dest='config_action')  # Add required=True if migrated to >=3.7

    show_config_parser = config_subparsers.add_parser(
        ACTION_CONFIG_SHOW,
        description='Print taros config or config specified by an option', add_help=False)
    show_config_parser.add_argument('-dc', '--def-config', action='store_true', help='show default config')

    create__config_parser = config_subparsers.add_parser(ACTION_CONFIG_CREATE,
                                                         description='create config file', add_help=False)

    create__config_parser.add_argument("--overwrite", action="store_true", help="overwrite config file to default")


def _init_parser_start(subparsers):
    """
    Creates parsers for `start` command

    :param subparsers: sub-parser for list-disabled parser to be added to
    """

    start_parser = subparsers.add_parser(ACTION_START, description='Start server', add_help=False)

    start_parser.add_argument('--set', type=str, action='append', help='override value of configuration field')
    start_parser.add_argument('-C', '--config', type=str, help='path to custom config file')
    start_parser.add_argument('-dc', '--def-config', action='store_true', help='ignore config files and use defaults')
