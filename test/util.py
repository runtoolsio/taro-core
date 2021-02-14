from multiprocessing.context import Process
from pathlib import Path
from typing import Dict, Tuple

import prompt_toolkit
import yaml
from prompt_toolkit.output import DummyOutput

from taro import app, program, paths, JobInfo, Warn, WarningObserver
from taro.job import WarnEventCtx


def run_app_as_process(command, daemon=False, shell=False) -> Process:
    p = Process(target=run_app, args=(command, shell), daemon=daemon)
    p.start()
    return p


def run_app(command, shell=False):
    """
    Run command
    :param command: command to run
    :param shell: use shell for executing command
    :return: output of the executed command
    """
    program.USE_SHELL = shell
    # Prevent UnsupportedOperation error: https://github.com/prompt-toolkit/python-prompt-toolkit/issues/1107
    prompt_toolkit.output.defaults.create_output = NoFormattingOutput
    try:
        app.main(command.split())
    finally:
        prompt_toolkit.output.defaults.create_output = None
        program.USE_SHELL = False


def run_wait(state, count=1) -> Process:
    """
    Run `wait` app. This app blocks until any job reaches the state defined in the `state` parameter.

    :param state: state for which the app waits
    :param count: number of waits
    :return: the app as a process
    """
    return run_app_as_process("wait -c {} {}".format(count, state.name))


def run_app_as_process_and_wait(command, *, wait_for, timeout=2, daemon=False, shell=False) -> Process:
    """
    Execute the command and wait for the job to reach the specified state.

    :param command: command to execute
    :param wait_for: state for which the execution wait
    :param timeout: waiting timeout
    :param daemon: whether the command as executed as a daemon process
    :param shell: execute the command using shell
    :return: the app as a process
    """

    pw = run_wait(wait_for)
    p = run_app_as_process(command, daemon, shell)
    pw.join(timeout)
    return p


def create_test_config(config):
    with open(_test_config_path(), 'w') as outfile:
        yaml.dump(config, outfile, default_flow_style=False)


def remove_test_config():
    config = _test_config_path()
    if config.exists():
        config.unlink()


def remove_test_db():
    test_db = test_db_path()
    if test_db.exists():
        test_db.unlink()


def _test_config_path() -> Path:
    base_path = Path(__file__).parent
    test_dir_path = base_path / paths.TEST_DIR
    test_dir_path.mkdir(exist_ok=True)
    return test_dir_path / paths.DEFAULT_CONFIG_FILE


def test_db_path() -> Path:
    base_path = Path(__file__).parent
    return base_path / 'test.db'


class NoFormattingOutput(DummyOutput):
    def write(self, data: str) -> None:
        print(data)

    def write_raw(self, data: str) -> None:
        print(data)

    def fileno(self) -> int:
        raise NotImplementedError()


class TestWarningObserver(WarningObserver):

    def __init__(self):
        self.warnings: Dict[str, Tuple[JobInfo, Warn, WarnEventCtx]] = {}

    def new_warning(self, job_info: JobInfo, warning: Warn, event_ctx):
        self.warnings[warning.name] = (job_info, warning, event_ctx)
