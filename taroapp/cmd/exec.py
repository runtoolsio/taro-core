import logging
import signal

from taro import util
from taro.jobs import warning
from taro.jobs.managed import ManagedJobContext
from taro.jobs.program import ProgramExecution
from taro.test.execution import TestExecution

logger = logging.getLogger(__name__)


def run(args):
    job_id = args.id or " ".join([args.command] + args.arg)
    if args.dry_run:
        execution = TestExecution(args.dry_run)
    else:
        execution = ProgramExecution([args.command] + args.arg, read_output=not args.bypass_output)

    with ManagedJobContext() as ctx:
        job_instance = ctx.create_job(
            job_id, execution, no_overlap=args.no_overlap, depends_on=args.depends_on, pending_value=args.pending,
            **(dict(args.param) if args.param else dict()))

        term = Term(job_instance)
        signal.signal(signal.SIGTERM, term.terminate)
        signal.signal(signal.SIGINT, term.interrupt)

        # TODO Move to managed
        for warn_time in args.warn_time:
            time = util.str_to_seconds(warn_time)
            warning.exec_time_exceeded(job_instance, f"exec_time>{time}s", time)

        for warn_output in args.warn_output:
            warning.output_matches(job_instance, f"output=~{warn_output}", warn_output)

        job_instance.run()

    if isinstance(execution, ProgramExecution) and execution.ret_code:
        if execution.ret_code > 0:
            raise ProgramExecutionError(execution.ret_code)
        if execution.ret_code < 0:
            raise ProgramExecutionError(abs(execution.ret_code) + 128)

    term_state = job_instance.lifecycle.state()
    if term_state.is_failure():
        raise ProgramExecutionError(1)


class Term:

    def __init__(self, job_instance):
        self.job_instance = job_instance

    def terminate(self, _, __):
        logger.warning('event=[terminated_by_signal]')
        self.job_instance.interrupt()

    def interrupt(self, _, __):
        logger.warning('event=[interrupted_by_keyboard]')
        self.job_instance.interrupt()


class ProgramExecutionError(SystemExit):
    pass
