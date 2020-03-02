from multiprocessing.context import Process
from pathlib import Path

import yaml

from taro import app


def run_app_as_process(command, daemon=False) -> Process:
    p = Process(target=run_app, args=(command,), daemon=daemon)
    p.start()
    return p


def run_app(command):
    """
    Run command with default config
    :param command: command to run
    :return: output of the executed command
    """
    app.FORCE_DEFAULT_CONFIG = True
    try:
        app.main(command.split())
    finally:
        app.FORCE_DEFAULT_CONFIG = False


def run_wait(state, count=1) -> Process:
    return run_app_as_process(f"wait -c {count} {state.name}")


def run_app_as_process_and_wait(command, *, wait_for, daemon=False) -> Process:
    pw = run_wait(wait_for)
    p = run_app_as_process(command, daemon)
    pw.join()
    return p


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
