import logging

from taro import cfg
from taro.jobs import lock, plugins
from taro.jobs.api import Server
from taro.jobs.events import StateDispatcher, OutputDispatcher
from taro.jobs.runner import RunnerJobInstance

log = logging.getLogger(__name__)

EXT_PLUGIN_MODULE_PREFIX = plugins.DEF_PLUGIN_MODULE_PREFIX


def create_managed_job(job_id, execution, state_locker=lock.default_state_locker(), *,
                       no_overlap=False, depends_on=None, pending_value=None, **params):
    job_instance = \
        RunnerJobInstance(job_id, execution, state_locker,
                          pending_value=pending_value, no_overlap=no_overlap, depends_on=depends_on, **params)

    if cfg.plugins:
        plugins.register_new_job_instance(job_instance, cfg.plugins, plugin_module_prefix=EXT_PLUGIN_MODULE_PREFIX)

    return ManagedJobInstance(job_instance)


class ManagedJobInstance:

    def __init__(self, job_instance):
        self.job_instance = job_instance

    def __call__(self, *args, **kwargs):
        # Send state events to external state listeners
        state_dispatcher = StateDispatcher()
        self.job_instance.add_state_observer(state_dispatcher)

        # Send output to external output listeners
        output_dispatcher = OutputDispatcher()
        self.job_instance.add_output_observer(output_dispatcher)

        api = Server()
        api.add_job_instance(self.job_instance)
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
