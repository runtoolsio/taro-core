"""
Public API of this package is imported here and it is safe to use by plugins.
Any API in sub-modules (except 'util' module) is a subject to change and doesn't ensure backwards compatibility.

IMPLEMENTATION NOTE:
    Avoid importing any module depending on any external package. This allows to use this module without installing
    additional packages.
"""
from .cfgfile import read_config
from .execution import ExecutionStateGroup, ExecutionState, ExecutionError, ExecutionLifecycle
from .hostinfo import read_hostinfo, HostinfoError
from .job import JobInstance, JobInfo, ExecutionStateObserver, Warn, WarningObserver, WarnEventCtx
from .managed import create_job_instance
from .paths import lookup_config_file_path
from .plugins import PluginBase, PluginDisabledError
from .util import NestedNamespace, format_timedelta


def execute(job_id, job_execution, *, no_overlap=False, pending_value=None):
    create_job_instance(job_id, job_execution, no_overlap=no_overlap, pending_value=pending_value)()
