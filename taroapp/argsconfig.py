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
    Overrides variable values in :mod:`cfg` module with values specified by CLI `set` option

    :param args: command line arguments
    """

    def split(s):
        if len(s) < 3 or "=" not in s[1:-1]:
            raise ValueError("Set option value must be in format: var=value")
        return s.split("=")

    if args.set:
        cfg.set_variables(**{k: v for k, v in (split(set_opt) for set_opt in args.set)})
