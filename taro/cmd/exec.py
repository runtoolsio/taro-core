import logging
import signal

from taro import cnf, ExecutionState, runner, PluginBase, warning
from taro import log
from taro import persistence
from taro.api import Server
from taro.listening import Dispatcher
from taro.process import ProcessExecution
from taro.runner import RunnerJobInstance

logger = logging.getLogger(__name__)

EXT_PLUGIN_MODULE_PREFIX = 'taro_'


def run(args):
    cnf.init(args)
    log.init()
    persistence.init()

    all_args = [args.command] + args.arg
    execution = ProcessExecution(all_args, read_output=not args.bypass_output)
    job_id = args.id or " ".join(all_args)
    job_instance = RunnerJobInstance(job_id, execution)
    term = Term(job_instance)
    signal.signal(signal.SIGTERM, term.terminate)
    signal.signal(signal.SIGINT, term.interrupt)
    dispatcher = Dispatcher()
    runner.register_state_observer(dispatcher)
    for plugin in PluginBase.create_plugins(EXT_PLUGIN_MODULE_PREFIX,
                                            cnf.config.plugins).values():  # TODO to plugin module
        try:
            plugin.new_job_instance(job_instance)
        except BaseException as e:
            logger.warning("event=[plugin_failed] reason=[exception_on_new_job_instance] detail=[%s]", e)

    if args.warn:
        warning.setup_checking(job_instance, *args.warn)

    pending_latch = \
        PendingLatch(args.pending, job_instance.create_latch(ExecutionState.PENDING)) if args.pending else None
    api = Server(job_instance, pending_latch)
    api_started = api.start()  # Starts a new thread
    if not api_started:
        logger.warning("event=[api_not_started] message=[Interface for managing the job failed to start]")
    try:
        job_instance.run()
    finally:
        api.stop()
        dispatcher.close()
        persistence.close()


class Term:

    def __init__(self, job_control):
        self.job_control = job_control

    def terminate(self, _, __):
        logger.warning('event=[terminated_by_signal]')
        self.job_control.interrupt()

    def interrupt(self, _, __):
        logger.warning('event=[interrupted_by_keyboard]')
        self.job_control.interrupt()  # TODO handle repeated signal


class PendingLatch:

    def __init__(self, value, latch):
        self.value = value
        self.latch = latch

    def release(self, value):
        if self.value == value:
            self.latch()
            return True
        else:
            return False
