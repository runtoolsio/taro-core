from collections.abc import Iterable

import yaml

from taro import util, paths
from taro.util import NestedNamespace, set_attr

LOG_ENABLED = 'log.enabled'
LOG_STDOUT_LEVEL = 'log.stdout.level'
LOG_FILE_LEVEL = 'log.file.level'
LOG_FILE_PATH = 'log.file.path'
PERSISTENCE_ENABLED = 'persistence.enabled'
PERSISTENCE_TYPE = 'persistence.type'
PERSISTENCE_DATABASE = 'persistence.database'
PLUGINS = 'plugins'

config = None


def init(args):
    global config
    config_file_path = get_config_file_path(args)
    config_ns = read_config(config_file_path)
    override_config(args, config_ns)
    config = Config(config_ns)


def get_config_file_path(args):
    if hasattr(args, 'config') and args.config:
        return util.expand_user(args.config)
    if hasattr(args, 'def_config') and args.def_config:
        return paths.default_config_file_path()
    if hasattr(args, 'min_config') and args.min_config:
        return paths.minimal_config_file_path()

    return paths.lookup_config_file()


def read_config(config_file_path) -> NestedNamespace:
    with open(config_file_path, 'r') as file:
        config_ns = util.wrap_namespace(yaml.safe_load(file))
        if config_ns:
            config_ns
        else:  # File is empty
            return NestedNamespace()


def override_config(args, config_ns: NestedNamespace):
    """
    Overrides values in configuration with cli option values for those specified on command line

    :param args: command line arguments
    :param config_ns: configuration
    """

    arg_to_config = {
        'log_enabled': LOG_ENABLED,
        'log_stdout': LOG_STDOUT_LEVEL,
        'log_file': LOG_FILE_LEVEL,
        'log_file_path': LOG_FILE_PATH,
    }

    for arg, conf in arg_to_config.items():
        if not hasattr(args, arg):
            continue
        arg_value = getattr(args, arg)
        if arg_value is not None:
            set_attr(config_ns, conf.split('.'), arg_value)


def print_config(args):
    config_file_path = get_config_file_path(args)
    print('Showing config file: ' + str(config_file_path))
    with open(config_file_path, 'r') as file:
        print(file.read())


class Config:

    def __init__(self, cns):
        self.log_enabled = cns.get(LOG_ENABLED, default=True)
        self.log_stdout_level = cns.get(LOG_STDOUT_LEVEL, default='off', type_=str).lower()
        self.log_file_level = cns.get(LOG_FILE_LEVEL, default='off', type_=str).lower()
        self.log_file_path = cns.get(LOG_FILE_PATH, type_=str)

        self.persistence_enabled = cns.get(PERSISTENCE_ENABLED, default=False)
        self.persistence_database = cns.get(PERSISTENCE_DATABASE)

        plugins = cns.get(PLUGINS)
        if isinstance(plugins, str):
            self.plugins = (plugins,)
        elif isinstance(plugins, Iterable):
            self.plugins = tuple(plugins)
        else:
            self.plugins = ()
