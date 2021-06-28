import logging

from taro import util

import taro.paths
from taros import cfg, paths

PORT = 'server.port'

LOG_ENABLED = 'log.enabled'
LOG_STDOUT_LEVEL = 'log.stdout.level'
LOG_FILE_LEVEL = 'log.file.level'
LOG_FILE_PATH = 'log.file.path'

log = logging.getLogger(__name__)


def load(config=None):
    config_path = util.expand_user(config) if config else taro.paths.lookup_file_in_config_path(paths.CONFIG_FILE)
    cns = util.read_yaml_file(config_path)
    log.debug("event=[config_file_loaded] path=[%s] content=[%s]", config_path, cns)

    cfg.port = cns.get(PORT, default=cfg.port, type_=int)
    cfg.log_enabled = cns.get(LOG_ENABLED, default=cfg.log_enabled, type_=bool)
    cfg.log_stdout_level = cns.get(LOG_STDOUT_LEVEL, default=cfg.log_stdout_level, type_=str).lower()
    cfg.log_file_level = cns.get(LOG_FILE_LEVEL, default=cfg.log_file_level, type_=str).lower()
    cfg.log_file_path = cns.get(LOG_FILE_PATH, default=cfg.log_file_path, type_=str)


def copy_default_file_to_search_path(overwrite: bool):
    cfg_to_copy = paths.default_config_file_path()
    # Copy to first dir in search path
    copy_to = taro.paths.config_file_search_path(exclude_cwd=True)[0] / paths.CONFIG_FILE
    util.copy_resource(cfg_to_copy, copy_to, overwrite)
