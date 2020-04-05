from multiprocessing.context import Process
from pathlib import Path

import prompt_toolkit
import yaml
from prompt_toolkit.output import DummyOutput

from taro import app, process


def run_app_as_process(command, daemon=False, shell=False) -> Process:
    p = Process(target=run_app, args=(command, shell), daemon=daemon)
    p.start()
    return p


def run_app(command, shell=False):
    """
    Run command with default config
    :param command: command to run
    :param shell: use shell for executing command
    :return: output of the executed command
    """
    app.USE_MINIMAL_CONFIG = True
    process.USE_SHELL = shell
    # Prevent UnsupportedOperation error: https://github.com/prompt-toolkit/python-prompt-toolkit/issues/1107
    prompt_toolkit.output.defaults.create_output = NoFormattingOutput
    try:
        app.main(command.split())
    finally:
        prompt_toolkit.output.defaults.create_output = None
        process.USE_SHELL = False
        app.USE_MINIMAL_CONFIG = False


def run_wait(state, count=1) -> Process:
    """
    Run `wait` app. This app blocks until any job reaches the state defined in the `state` parameter.

    :param state: state for which the app waits
    :param count: number of waits
    :return: the app as a process
    """
    return run_app_as_process("wait -c {} {}".format(count, state.name))


def run_app_as_process_and_wait(command, *, wait_for, daemon=False, shell=False) -> Process:
    """
    Execute the command and wait for the job to reach the specified state.

    :param command: command to execute
    :param wait_for: state for which the execution wait
    :param daemon: whether the command as executed as a daemon process
    :param shell: execute the command using shell
    :return: the app as a process
    """

    pw = run_wait(wait_for)
    p = run_app_as_process(command, daemon, shell)
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
