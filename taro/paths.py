import os
from pathlib import Path

_CONFIG_FILE = 'taro.yaml'
_LOG_FILE = 'taro.log'


def _is_root():
    return os.geteuid() == 0


def config_file_path() -> Path:
    paths = []

    if not _is_root():
        home_dir = Path.home()
        user_config = home_dir / '.config' / 'taro' / _CONFIG_FILE
        paths.append(user_config)
        if user_config.exists():
            return user_config

    system_config = Path('/etc/taro') / _CONFIG_FILE
    paths.append(system_config)
    if system_config.exists():
        return system_config

    raise FileNotFoundError('None config file found: ' + str([str(config) for config in paths]))


def log_file_path() -> Path:
    if _is_root():
        path = Path('/var/log/taro')
    else:
        home = Path.home()
        path = home / '.cache' / 'taro'

    path.mkdir(parents=True, exist_ok=True)
    return path / 'taro.log'
