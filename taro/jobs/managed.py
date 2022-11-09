import logging

from taro import cfg
from taro.jobs import lock
from taro.jobs.api import Server
from taro.jobs.events import StateDispatcher, OutputDispatcher
from taro.jobs.execution import ExecutionState
from taro.jobs.plugins import PluginBase
from taro.jobs.runner import RunnerJobInstance

log = logging.getLogger(__name__)


def create_managed_job(job_id, execution, state_locker=lock.default_state_locker(), *,
                       no_overlap=False, depends_on=None, pending_value=None, **params):
    job_instance =\
        RunnerJobInstance(job_id, execution, state_locker, no_overlap=no_overlap, depends_on=depends_on, **params)

    if cfg.plugins:
        PluginBase.load_plugins(EXT_PLUGIN_MODULE_PREFIX, cfg.plugins, reload=False)  # Load plugins if not yet loaded
    for plugin in PluginBase.name2plugin.values():  # May contain other plugins loaded before
        try:
            plugin.new_job_instance(job_instance)
        except BaseException as e:
            log.warning("event=[plugin_failed] reason=[exception_on_new_job_instance] detail=[%s]", e)

    job = ManagedJobInstance(job_instance)
    job.pending_value = pending_value
    return job


class ManagedJobInstance:

    def __init__(self, job_instance):
        self.job_instance = job_instance
        self.pending_value = None

    def __call__(self, *args, **kwargs):
        # Send state events to external state listeners
        state_dispatcher = StateDispatcher()
        self.job_instance.add_state_observer(state_dispatcher)

        # Send output to external output listeners
        output_dispatcher = OutputDispatcher()
        self.job_instance.add_output_observer(output_dispatcher)

        if self.pending_value:
            latch = _PendingValueLatch(self.pending_value, self.job_instance.create_latch(ExecutionState.PENDING))
        else:
            latch = None
        api = Server(self.job_instance, latch)
        api_started = api.start()  # Starts a new thread
        if not api_started:
            log.warning("event=[api_not_started] message=[Interface for managing the job failed to start]")

        try:
            self.job_instance.run()
        finally:
            for c in api, output_dispatcher, state_dispatcher:
                # noinspection PyBroadException
                try:
                    c.close()
                except BaseException:
                    log.exception("event=[failed_to_close_resource] resource=[%s]", c)


class _PendingValueLatch:

    def __init__(self, value, latch):
        self.value = value
        self.latch = latch

    def release(self, value):
        if self.value == value:
            self.latch()
            return True
        else:
            return False


EXT_PLUGIN_MODULE_PREFIX = 'taro_'