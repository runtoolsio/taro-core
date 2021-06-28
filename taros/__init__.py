import daemon

import taro.paths
from taro import util

from taros import paths, cli, cfgfile, app


def main_cli():
    main(None)


def main(args):
    """
    Taro Server (taros) app main function.
    :param args: CLI arguments
    """
    args_ns = cli.parse_args(args)

    if args_ns.action == cli.ACTION_CONFIG:
        run_config(args_ns)
    elif args_ns.action == cli.ACTION_START:
        init_server(args_ns)
        start_server(args_ns)


def run_config(args):
    if args.config_action == cli.ACTION_CONFIG_SHOW:
        if getattr(args, 'def_config', False):
            util.print_file(paths.default_config_file_path())
        else:
            util.print_file(taro.paths.lookup_file_in_config_path(paths.CONFIG_FILE))
    elif args.config_action == cli.ACTION_CONFIG_CREATE:
        cfgfile.copy_default_file_to_search_path(args.overwrite)


def init_server(args):
    config_vars = util.split_params(args.set) if args.set else {}  # TODO Config variables and override values

    if getattr(args, 'config', None):
        cfgfile.load(args.config)
    elif getattr(args, 'def_config', False):
        cfgfile.load(paths.default_config_file_path())
    else:
        cfgfile.load()

    taro.load_defaults()  # TODO support for custom taro config


def start_server(args):
    if args.daemon:
        with daemon.DaemonContext():
            app.start()
    else:
        app.start()
