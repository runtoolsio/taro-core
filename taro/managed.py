import logging

from taro import PluginBase, ExecutionState
from taro.api import Server
from taro.listening import StateDispatcher, OutputDispatcher
from taro.runner import RunnerJobInstance

logger = logging.getLogger(__name__)


def create_managed_job(job_id, execution, *, no_overlap=False, pending_value=None):
    job_instance = RunnerJobInstance(job_id, execution, no_overlap=no_overlap)

    # Forward output from execution to the job instance for the instance's output listeners
    execution.add_output_observer(job_instance)

    # Send state events to external state listeners
    state_dispatcher = StateDispatcher()
    job_instance.add_state_observer(state_dispatcher)

    # Send output to external output listeners
    output_dispatcher = OutputDispatcher()
    job_instance.add_output_observer(output_dispatcher)

    for plugin in PluginBase.name2plugin.values():
        try:
            plugin.new_job_instance(job_instance)
        except BaseException as e:
            logger.warning("event=[plugin_failed] reason=[exception_on_new_job_instance] detail=[%s]", e)

    if pending_value:
        latch = _PendingValueLatch(pending_value, job_instance.create_latch(ExecutionState.PENDING))
    else:
        latch = None
    api = Server(job_instance, latch)

    def run():
        try:
            job_instance.run()
        finally:
            api.stop()
            output_dispatcher.close()
            state_dispatcher.close()

    return run


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
