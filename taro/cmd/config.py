"""
Commands related to configuration.
Show: shows path and content of the config file
"""

from taro import cli, cfgfile, argsconfig


def run(args):
    config_path = argsconfig.get_config_file_path(args)

    if args.config_action == cli.ACTION_CONFIG_SHOW:
        cfgfile.print_config(config_path)
    if args.config_action == cli.ACTION_CONFIG_PATH:
        print(config_path)
