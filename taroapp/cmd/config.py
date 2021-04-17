"""
Commands related to configuration.
Show: shows path and content of the config file
"""
from shutil import copy
from pathlib import Path


from taro import cfgfile, paths
from taroapp import argsconfig, cli



def run(args):
    if args.config_action == cli.ACTION_CONFIG_SHOW:
        config_path = argsconfig.get_config_file_path(args)
        cfgfile.print_config(config_path)
    if args.config_action == cli.ACTION_CONFIG_CREATE:
        create(args)
    if args.config_action == cli.ACTION_CONFIG_PATH:
        config_path = argsconfig.get_config_file_path(args)
        print(config_path)


def create(args):
    current_dir_path = Path.cwd() / 'taro.yaml'

    if not current_dir.exists() or args.overwrite:
        default_config_path = paths.default_config_file_path()
        print("creating config file in " + str(current_dir_path))
        copy(default_config_path, current_dir_path)
        print("done!")
        return

    raise FileExistsError('File alredy exist in: ' + str(current_dir))