"""
Public API of this package is imported here, and it is safe to use by plugins.
Any API in submodules (except 'util' module) is a subject to change and doesn't ensure backwards compatibility.

IMPLEMENTATION NOTE:
    Avoid importing any module depending on any external package. This allows to use this module without installing
    additional packages.
"""
from threading import Thread

import tarotools.taro.cfg
from tarotools.taro import cfg, client, log
from tarotools.taro.hostinfo import read_hostinfo, HostinfoError
from tarotools.taro.jobs import warning, persistence, plugins, repo, sync
from tarotools.taro.jobs.execution import Flag, ExecutionState, ExecutionError, ExecutionLifecycle
from tarotools.taro.jobs.inst import JobInstanceID, JobInstance, JobInst, ExecutionStateObserver, Warn, WarningObserver, \
    WarnEventCtx
from tarotools.taro.jobs.managed import ManagedJobContext
from tarotools.taro.jobs.plugins import PluginBase, PluginDisabledError
from tarotools.taro.jobs.process import ProcessExecution
from tarotools.taro.jobs.program import ProgramExecution
from tarotools.taro.jobs.runner import RunnerJobInstance
from tarotools.taro.paths import lookup_file_in_config_path
from tarotools.taro.util import format_timedelta, read_toml_file_flatten

__version__ = "0.11.0"


def load_config(config=None, **kwargs):
    cfg.load_from_file(config)
    configure(**kwargs)


def configure(**kwargs):
    cfg.set_variables(**kwargs)
    log.init_by_config()


def execute(job_id, job_execution, instance_id=None, *, no_overlap=False, depends_on=None, pending_group=None):
    with ManagedJobContext() as ctx:
        job_instance = ctx.add(RunnerJobInstance(
            job_id,
            job_execution,
            sync.create_composite(no_overlap=no_overlap, depends_on=depends_on),
            instance_id=instance_id,
            pending_group=pending_group))
        job_instance.run()


def execute_in_new_thread(job_id, job_execution, no_overlap=False, depends_on=None, pending_group=None):
    Thread(target=execute, args=(job_id, job_execution, no_overlap, depends_on, pending_group)).start()


def close():
    persistence.close()
