from tarotools.taro import paths
from tarotools.taro import util, cfg
from tarotools.taro.err import TaroException, ConfigFileNotFoundError

loaded_config_path = None


def load(config=None):
    config_path = util.expand_user(config) if config else paths.lookup_config_file()
    try:
        flatten_cfg = util.read_toml_file_flatten(config_path)
    except FileNotFoundError:
        # Must be the explicit `config` as `lookup_config_file` already raises this exception
        raise ConfigFileNotFoundError(config)

    cfg.set_variables(**flatten_cfg)

    global loaded_config_path
    loaded_config_path = config_path


def copy_default_file_to_search_path(overwrite: bool):
    cfg_to_copy = paths.default_config_file_path()
    # Copy to first dir in search path
    # TODO Specify where to copy the file - do not use XDG search path
    copy_to = paths.taro_config_file_search_path(exclude_cwd=True)[0] / paths.CONFIG_FILE
    try:
        util.copy_resource(cfg_to_copy, copy_to, overwrite)
        return copy_to
    except FileExistsError as e:
        raise ConfigFileAlreadyExists(str(e)) from e


class ConfigFileAlreadyExists(TaroException, FileExistsError):
    pass
