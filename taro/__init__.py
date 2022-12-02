"""
Public API of this package is imported here and it is safe to use by plugins.
Any API in sub-modules (except 'util' module) is a subject to change and doesn't ensure backwards compatibility.

IMPLEMENTATION NOTE:
    Avoid importing any module depending on any external package. This allows to use this module without installing
    additional packages.
"""
from . import cfg, cfgfile, log
from .hostinfo import read_hostinfo, HostinfoError
from .jobs import warning, persistence, repo, sync
from .jobs.execution import ExecutionStateGroup, ExecutionState, ExecutionError, ExecutionLifecycle
from .jobs.job import JobInstanceID, JobInstance, JobInfo, ExecutionStateObserver, Warn, WarningObserver, WarnEventCtx
from .jobs.managed import ManagedJobContext
from .jobs.plugins import PluginBase, PluginDisabledError
from .jobs.process import ProcessExecution
from .jobs.program import ProgramExecution
from .jobs.runner import RunnerJobInstance
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


def execute(job_id, job_execution, no_overlap=False, depends_on=None, pending_group=None):
    with ManagedJobContext() as ctx:
        job_instance = ctx.add(RunnerJobInstance(
            job_id,
            job_execution,
            sync.create_composite(no_overlap=no_overlap, depends_on=depends_on),
            pending_group=pending_group))
        job_instance.run()


def close():
    persistence.close()
