import logging

import taro.paths
from taro import util
from taros import cfg, paths

SERVER_BIND = 'server.bind'
SERVER_PORT = 'server.port'

LOG_ENABLED = 'log.enabled'
LOG_STDOUT_LEVEL = 'log.stdout.level'
LOG_FILE_LEVEL = 'log.file.level'
LOG_FILE_PATH = 'log.file.path'

log = logging.getLogger(__name__)


def load(config=None):
    config_path = util.expand_user(config) if config else taro.paths.lookup_file_in_config_path(paths.CONFIG_FILE)
    cnf = util.read_yaml_file(config_path)
    log.debug("event=[config_file_loaded] path=[%s] content=[%s]", config_path, cnf)

    cfg.server_bind = cnf.get(SERVER_BIND, default=cfg.server_bind, type_=str, allowed=('localhost', 'all')).lower()
    cfg.server_port = cnf.get(SERVER_PORT, default=cfg.server_port, type_=int)
    cfg.log_enabled = cnf.get(LOG_ENABLED, default=cfg.log_enabled, type_=bool)
    cfg.log_stdout_level = cnf.get(LOG_STDOUT_LEVEL, default=cfg.log_stdout_level, type_=str).lower()
    cfg.log_file_level = cnf.get(LOG_FILE_LEVEL, default=cfg.log_file_level, type_=str).lower()
    cfg.log_file_path = cnf.get(LOG_FILE_PATH, default=cfg.log_file_path, type_=str)


def copy_default_file_to_search_path(overwrite: bool):
    cfg_to_copy = paths.default_config_file_path()
    # Copy to first dir in search path
    copy_to = taro.paths.taro_config_file_search_path(exclude_cwd=True)[0] / paths.CONFIG_FILE
    util.copy_resource(cfg_to_copy, copy_to, overwrite)
