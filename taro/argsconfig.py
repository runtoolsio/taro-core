"""
Reads CLI arguments to initialize and modify :mod:`cfg` module during initialization phase of the application.
"""

from taro import paths, cfgfile, cfg


def load_config(args):
    if not getattr(args, 'min_config', False):
        cfgfile.load(get_config_file_path(args))


def get_config_file_path(args):
    if getattr(args, 'config', None):
        return args.config
    if getattr(args, 'def_config', False):
        return paths.default_config_file_path()

    return paths.lookup_config_file()


def override_config(args):
    """
    Overrides values in :mod:`cfg` module with cli option values for those specified on command line

    :param args: command line arguments
    """

    arg2config_attr = {
        'log_enabled': 'log_enabled',
        'log_stdout': 'log_stdout_level',
        'log_file': 'log_file_level',
        'log_file_path': 'log_file_path',
    }

    for arg, cfg_attr in arg2config_attr.items():
        arg_value = getattr(args, arg, None)
        if not hasattr(cfg, cfg_attr):
            raise AttributeError("Module `cfg` does not have attribute: " + cfg_attr)
        if arg_value is not None:
            setattr(cfg, cfg_attr, arg_value)
