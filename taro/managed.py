import logging

from taro import PluginBase, ExecutionState
from taro.api import Server
from taro.listening import StateDispatcher, OutputDispatcher
from taro.runner import RunnerJobInstance

log = logging.getLogger(__name__)


def create_job_instance(job_id, execution, *, no_overlap=False, pending_value=None):
    job_instance = RunnerJobInstance(job_id, execution, no_overlap=no_overlap)

    # Forward output from execution to the job instance for the instance's output listeners
    execution.add_output_observer(job_instance)

    for plugin in PluginBase.name2plugin.values():
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

        closeable = [api, output_dispatcher, state_dispatcher]
        try:
            self.job_instance.run()
        finally:
            for c in closeable:
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
