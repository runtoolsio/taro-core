from pathlib import Path

import yaml

from taro import app


def run_app(command):
    """
    Run command
    :param command: command to run
    :return: output of the executed command
    """
    app.main(command.split())


def create_test_config(config):
    with open(_test_config_path(), 'w') as outfile:
        yaml.dump(config, outfile, default_flow_style=False)


def remove_test_config():
    config = _test_config_path()
    if config.exists():
        config.unlink()


def _test_config_path() -> Path:
    base_path = Path(__file__).parent
    return base_path / 'test.yaml'
