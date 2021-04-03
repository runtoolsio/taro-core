import logging
from collections import Iterable

import yaml

from taro import cfg, util, paths
from taro.util import NestedNamespace

LOG_ENABLED = 'log.enabled'
LOG_STDOUT_LEVEL = 'log.stdout.level'
LOG_FILE_LEVEL = 'log.file.level'
LOG_FILE_PATH = 'log.file.path'
PERSISTENCE_ENABLED = 'persistence.enabled'
PERSISTENCE_TYPE = 'persistence.type'
PERSISTENCE_DATABASE = 'persistence.database'
PLUGINS = 'plugins'

log = logging.getLogger(__name__)


def load(config=None):
    config_path = util.expand_user(config) if config else paths.lookup_config_file()
    cns = read_config(config_path)
    log.debug("event=[config_file_loaded] path=[%s] content=[%s]", config_path, cns)

    cfg.log_enabled = cns.get(LOG_ENABLED, default=cfg.log_enabled, type_=bool)
    cfg.log_stdout_level = cns.get(LOG_STDOUT_LEVEL, default=cfg.log_stdout_level, type_=str).lower()
    cfg.log_file_level = cns.get(LOG_FILE_LEVEL, default=cfg.log_file_level, type_=str).lower()
    cfg.log_file_path = cns.get(LOG_FILE_PATH, default=cfg.log_file_path, type_=str)

    cfg.persistence_enabled = cns.get(PERSISTENCE_ENABLED, default=cfg.persistence_enabled, type_=bool)
    cfg.persistence_type = cns.get(PERSISTENCE_TYPE, default=cfg.persistence_type, type_=str)
    cfg.persistence_database = cns.get(PERSISTENCE_DATABASE, default=cfg.persistence_database, type_=str)

    plugins = cns.get(PLUGINS)
    if isinstance(plugins, str):
        cfg.plugins = (plugins,)
    elif isinstance(plugins, Iterable):
        cfg.plugins = tuple(plugins)


def read_config(config_file_path) -> NestedNamespace:
    with open(config_file_path, 'r') as file:
        config_ns = util.wrap_namespace(yaml.safe_load(file))
        if config_ns:
            return config_ns
        else:  # File is empty
            return NestedNamespace()


def print_config(config=None):
    config_path = util.expand_user(config) if config else paths.lookup_config_file()
    print('Showing config file: ' + str(config_path))
    with open(config_path, 'r') as file:
        print(file.read())
