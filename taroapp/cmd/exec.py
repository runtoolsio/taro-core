import logging
import signal

from taro.jobs import sync, warning
from taro.jobs.managed import ManagedJobContext
from taro.jobs.program import ProgramExecution
from taro.jobs.runner import RunnerJobInstance
from taro.jobs.sync import ExecutionsLimit
from taro.test.execution import TestExecution

logger = logging.getLogger(__name__)


def run(args):
    if args.dry_run:
        execution = TestExecution(args.dry_run)
    else:
        execution = ProgramExecution([args.command] + args.arg, read_output=not args.bypass_output)

    job_id = args.id or " ".join([args.command] + args.arg)
    if args.serial:
        exec_limit = ExecutionsLimit(args.execution_group or job_id, 1)
    elif args.max_executions:
        exec_limit = ExecutionsLimit(args.execution_group or job_id, args.max_executions)
    else:
        exec_limit = None
    job_instance = RunnerJobInstance(
        job_id,
        execution,
        sync.create_composite(executions_limit=exec_limit, no_overlap=args.no_overlap, depends_on=args.depends_on),
        pending_group=args.pending,
        **(dict(args.param) if args.param else dict()))

    warning.register(job_instance, warn_times=args.warn_time, warn_outputs=args.warn_output)

    term = Term(job_instance)
    signal.signal(signal.SIGTERM, term.terminate)
    signal.signal(signal.SIGINT, term.interrupt)

    with ManagedJobContext() as ctx:
        ctx.add(job_instance)
        job_instance.run()

    if isinstance(execution, ProgramExecution) and execution.ret_code:
        if execution.ret_code > 0:
            raise ProgramExecutionError(execution.ret_code)
        if execution.ret_code < 0:
            raise ProgramExecutionError(abs(execution.ret_code) + 128)

    term_state = job_instance.lifecycle.state
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
