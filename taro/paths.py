import os
from pathlib import Path

_CONFIG_FILE = 'taro.yaml'


def _is_root():
    return os.geteuid() == 0


def config_file_path() -> Path:
    configs = []

    if not _is_root():
        home_dir = Path.home()
        user_config = home_dir / '.config' / 'taro' / _CONFIG_FILE
        configs.append(user_config)
        if user_config.exists():
            return user_config

    system_config = Path('/etc/taro') / _CONFIG_FILE
    configs.append(system_config)
    if system_config.exists():
        return system_config

    raise FileNotFoundError('None config file found: ' + str([str(config) for config in configs]))
