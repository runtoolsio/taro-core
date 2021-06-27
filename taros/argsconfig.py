"""
Reads CLI arguments to initialize and modify :mod:`cfg` module during initialization phase of the application.
"""

import taro.paths
from . import cfgfile
from taros import cfg, paths


def get_config_file_path(args):
    if getattr(args, 'config', None):
        return args.config
    if getattr(args, 'def_config', False):
        return paths.default_config_file_path()

    return taro.paths.lookup_file_in_config_path(paths.CONFIG_FILE)


def override_config(args):
    """
    Overrides variable values in :mod:`cfg` module with values specified by CLI `set` option

    :param args: command line arguments
    """

    def split(s):
        if len(s) < 3 or "=" not in s[1:-1]:
            raise ValueError("Set option value must be in format: var=value")
        return s.split("=")

    if args.set:
        cfg.set_variables(**{k: v for k, v in (split(set_opt) for set_opt in args.set)})
