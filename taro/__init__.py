"""
Public API of this package is imported here and it is safe to use by plugins.
Any API in sub-modules (except 'util' module) is a subject to change and doesn't ensure backwards compatibility.

IMPLEMENTATION NOTE:
    Avoid importing any module depending on any external package. This allows to use this module without installing
    additional packages.
"""
import os
from . import cfg, cfgfile, log
from .hostinfo import read_hostinfo, HostinfoError
from .jobs import warning, persistence
from .jobs.execution import ExecutionStateGroup, ExecutionState, ExecutionError, ExecutionLifecycle
from .jobs.job import JobInstance, JobInfo, ExecutionStateObserver, Warn, WarningObserver, WarnEventCtx
from .jobs.managed import create_managed_job
from .jobs.plugins import PluginBase, PluginDisabledError
from .jobs.process import ProcessExecution
from .jobs.program import ProgramExecution
from .paths import lookup_file_in_config_path
from .util import NestedNamespace, format_timedelta, read_yaml_file


def load_defaults(**kwargs):
    cfgfile.load(paths.default_config_file_path())
    setup(**kwargs)


def load_config(config=None, **kwargs):
    cfgfile.load(config)
    setup(**kwargs)


def setup(**kwargs):
    cfg.set_variables(**kwargs)
    log.init_by_config()


def managed_job(job_id, job_execution, *ext, no_overlap=False, pending_value=None):
    mng_job = create_managed_job(job_id, job_execution, no_overlap=no_overlap, pending_value=pending_value)
    for extension in ext:
        extension(mng_job.job_instance)
    return mng_job


def execute(job_id, job_execution, *ext, no_overlap=False, pending_value=None):
    managed_job(job_id, job_execution, *ext, no_overlap=no_overlap, pending_value=pending_value)()


def exec_time_warning(time: float):
    return lambda job_instance: warning.exec_time_exceeded(job_instance, f"exec_time>{time}s", time)


def output_warning(regex: str):
    return lambda job_instance: warning.output_matches(job_instance, f"output=~{regex}", regex)


def auto_init():
    path = paths.config_file_search_path(exclude_cwd=True)[0]
    if not os.path.exists(path / '.init'):
        open(path / '.init', 'a').close()
        cfgfile.copy_default_file_to_search_path(overwrite=False)
        print("Taro initialized")


def close():
    persistence.close()
