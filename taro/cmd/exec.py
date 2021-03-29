import logging
import signal

from taro import log
from taro import persistence
from taro import warning, managed, PluginBase, cfg
from taro.program import ProgramExecution
from taro.test.execution import TestExecution

logger = logging.getLogger(__name__)

EXT_PLUGIN_MODULE_PREFIX = 'taro_'


def run(args):
    log.init()
    persistence.init()
    PluginBase.load_plugins(EXT_PLUGIN_MODULE_PREFIX, cfg.plugins)

    job_id = args.id or " ".join([args.command] + args.arg)
    if args.dry_run:
        execution = TestExecution(args.dry_run)
    else:
        execution = ProgramExecution([args.command] + args.arg, read_output=not args.bypass_output)

    managed_job = managed.create_job_instance(job_id, execution, no_overlap=args.no_overlap, pending_value=args.pending)

    term = Term(managed_job.job_instance)
    signal.signal(signal.SIGTERM, term.terminate)
    signal.signal(signal.SIGINT, term.interrupt)

    if args.warn:
        warning.setup_checking(managed_job.job_instance, *args.warn)

    try:
        managed_job()
    finally:
        persistence.close()


class Term:

    def __init__(self, job_instance):
        self.job_instance = job_instance

    def terminate(self, _, __):
        logger.warning('event=[terminated_by_signal]')
        self.job_instance.interrupt()

    def interrupt(self, _, __):
        logger.warning('event=[interrupted_by_keyboard]')
        self.job_instance.interrupt()  # TODO handle repeated signal
