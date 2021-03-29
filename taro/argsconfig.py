"""
Reads CLI arguments to initialize and modify :mod:`cfg` module during initialization phase of the application.
"""

from taro import paths, cfgfile, cfg


def load_config(args):
    if not args.get('min_config'):
        cfgfile.load(_get_config_file_path(args))


def _get_config_file_path(args):
    if args.get('config'):
        return args.config
    if args.get('def_config'):
        return paths.default_config_file_path()

    return paths.lookup_config_file()


def override_config(args):
    """
    Overrides values in :mod:`cfg` module with cli option values for those specified on command line

    :param args: command line arguments
    """

    arg2config_var = {
        'log_enabled': 'log.enabled',
        'log_stdout': 'log.stdout.level',
        'log_file': 'log.file.level',
        'log_file_path': 'log.file.path',
    }

    for arg, conf_var in arg2config_var.items():
        arg_value = args.get(arg)
        if arg_value is not None:
            setattr(cfg, conf_var, arg_value)
