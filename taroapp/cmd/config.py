"""
Commands related to configuration.
Show: shows path and content of the config file
"""
from taro import cfgfile, util
from taroapp import argsconfig, cli


def run(args):
    if args.config_action == cli.ACTION_CONFIG_SHOW:
        config_path = argsconfig.get_config_file_path(args)
        util.print_file(config_path)
    if args.config_action == cli.ACTION_CONFIG_CREATE:
        cfgfile.create(args.overwrite)
    if args.config_action == cli.ACTION_CONFIG_PATH:
        config_path = argsconfig.get_config_file_path(args)
        print(config_path)
