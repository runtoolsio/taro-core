"""
Commands related to configuration.
Show: shows path and content of the config file
"""
from taro import util, paths
from taroapp import argsconfig, cli


def run(args):
    if args.config_action == cli.ACTION_CONFIG_SHOW:
        config_path = argsconfig.get_config_file_path(args)
        util.print_file(config_path)
    if args.config_action == cli.ACTION_CONFIG_CREATE:
        cfg_to_copy = paths.default_config_file_path()
        # Copy to first dir in search path
        copy_to = paths.config_file_search_path(exclude_cwd=True)[0] / paths.CONFIG_FILE
        util.copy_resource(cfg_to_copy, copy_to, args.overwrite)
    if args.config_action == cli.ACTION_CONFIG_PATH:
        config_path = argsconfig.get_config_file_path(args)
        print(config_path)
