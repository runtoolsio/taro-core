from datetime import datetime, timedelta
from multiprocessing import Queue
from pathlib import Path

import tomli_w

from tarotools.taro import cfg, util
from tarotools.taro import paths, JobRun, PhaseTransitionObserver
from tarotools.taro.jobs.instance import JobInstanceMetadata
from tarotools.taro.run import Run, PhaseMetadata, RunState, Lifecycle, PhaseRun, StandardPhaseNames, TerminationInfo, \
    TerminationStatus, RunFailure


def run(job_id, offset_minutes=0):
    now = datetime.utcnow()
    start_time = now.replace(microsecond=0) + timedelta(minutes=offset_minutes)

    lifecycle_phases = [
        PhaseRun(StandardPhaseNames.INIT, RunState.CREATED, start_time, start_time + timedelta(minutes=1)),
        PhaseRun(StandardPhaseNames.APPROVAL, RunState.EXECUTING, start_time + timedelta(minutes=1),
                 start_time + timedelta(minutes=2)),
        PhaseRun(StandardPhaseNames.PROGRAM, RunState.EXECUTING, start_time + timedelta(minutes=2),
                 start_time + timedelta(minutes=3)),
        PhaseRun(StandardPhaseNames.TERMINAL, RunState.ENDED, start_time + timedelta(minutes=3), None),
    ]
    lifecycle = Lifecycle(*lifecycle_phases)

    termination_info = TerminationInfo(TerminationStatus.FAILED, start_time + timedelta(minutes=3),
                                       RunFailure('err1', 'reason'))
    run_ = Run((PhaseMetadata('p1', RunState.EXECUTING, {'p': 'v'}),), lifecycle, termination_info)
    metadata = JobInstanceMetadata(job_id, 'r1', util.unique_timestamp_hex(), {}, {'name': 'value'})

    return JobRun(metadata, run_, None)


def reset_config():
    cfg.log_mode = cfg.DEF_LOG
    cfg.log_stdout_level = cfg.DEF_LOG_STDOUT_LEVEL
    cfg.log_file_level = cfg.DEF_LOG_FILE_LEVEL
    cfg.log_file_path = cfg.DEF_LOG_FILE_PATH

    cfg.persistence_enabled = cfg.DEF_PERSISTENCE_ENABLED
    cfg.persistence_type = cfg.DEF_PERSISTENCE_TYPE
    cfg.persistence_database = cfg.DEF_PERSISTENCE_DATABASE

    cfg.plugins_load = cfg.DEF_PLUGINS_LOAD


def create_test_config(config):
    create_custom_test_config(paths.CONFIG_FILE, config)


def create_custom_test_config(filename, config):
    path = _custom_test_config_path(filename)
    with open(path, 'wb') as outfile:
        tomli_w.dump(config, outfile)
    return path


def remove_test_config():
    remove_custom_test_config(paths.CONFIG_FILE)


def remove_custom_test_config(filename):
    config = _custom_test_config_path(filename)
    if config.exists():
        config.unlink()


def _test_config_path() -> Path:
    return _custom_test_config_path(paths.CONFIG_FILE)


def _custom_test_config_path(filename) -> Path:
    return Path.cwd() / filename


class StateWaiter:
    """
    This class is used for waiting for execution states of job executed in different process.

    See :class:`PutStateToQueueObserver`

    Attributes:
        state_queue The process must put execution states into this queue
    """

    def __init__(self):
        self.state_queue = Queue()

    def wait_for_state(self, state, timeout=1):
        while True:
            if state == self.state_queue.get(timeout=timeout):
                return


class PutPhaseToQueueObserver(PhaseTransitionObserver):
    """
    This observer puts execution states into the provided queue. With multiprocessing queue this can be used for sending
    execution states into the parent process.

    See :class:`StateWaiter`
    """

    def __init__(self, queue):
        self.queue = queue

    def new_phase(self, job_run: JobRun, previous_phase, new_phase, changed):
        self.queue.put_nowait(new_phase)
