from multiprocessing.context import Process
from pathlib import Path

import prompt_toolkit
import yaml
from prompt_toolkit.output import DummyOutput

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
    app.USE_MINIMAL_CONFIG = True
    # Prevent UnsupportedOperation error: https://github.com/prompt-toolkit/python-prompt-toolkit/issues/1107
    prompt_toolkit.output.defaults.create_output = NoFormattingOutput
    try:
        app.main(command.split())
    finally:
        app.USE_MINIMAL_CONFIG = False
        prompt_toolkit.output.defaults.create_output = None


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


class NoFormattingOutput(DummyOutput):
    def write(self, data: str) -> None:
        print(data)

    def write_raw(self, data: str) -> None:
        print(data)

    def fileno(self) -> int:
        raise NotImplementedError()
