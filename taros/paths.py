from pathlib import Path

CONFIG_FILE = 'taros.yaml'


def default_config_file_path() -> Path:
    return config_file_path(CONFIG_FILE)


def config_file_path(filename) -> Path:
    base_path = Path(__file__).parent
    def_config = base_path / 'config' / filename
    if not def_config.exists():
        raise FileNotFoundError(filename + ' config file not found, corrupted installation?')
    return def_config
