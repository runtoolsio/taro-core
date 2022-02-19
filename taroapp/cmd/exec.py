import logging
import signal

import taro
from taro import util
from taro.jobs.program import ProgramExecution
from taro.test.execution import TestExecution

logger = logging.getLogger(__name__)


def run(args):
    job_id = args.id or " ".join([args.command] + args.arg)
    if args.dry_run:
        execution = TestExecution(args.dry_run)
    else:
        execution = ProgramExecution([args.command] + args.arg, read_output=not args.bypass_output)
    extensions = []
    for warn_time in args.warn_time:
        extensions.append(taro.exec_time_warning(util.str_to_seconds(warn_time)))
    for warn_output in args.warn_output:
        extensions.append(taro.output_warning(warn_output))

    managed_job = taro.managed_job(job_id, execution, *extensions, no_overlap=args.no_overlap,
                                   pending_value=args.pending, **(dict(args.param) if args.param else dict()))

    term = Term(managed_job.job_instance)
    signal.signal(signal.SIGTERM, term.terminate)
    signal.signal(signal.SIGINT, term.interrupt)

    managed_job()


class Term:

    def __init__(self, job_instance):
        self.job_instance = job_instance

    def terminate(self, _, __):
        logger.warning('event=[terminated_by_signal]')
        self.job_instance.interrupt()

    def interrupt(self, _, __):
        logger.warning('event=[interrupted_by_keyboard]')
        self.job_instance.interrupt()  # TODO handle repeated signal
