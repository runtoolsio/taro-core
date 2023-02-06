import logging
import signal

from pygrok import Grok

from taro.jobs import sync, warning
from taro.jobs.job import Warn
from taro.jobs.managed import ManagedJobContext
from taro.jobs.program import ProgramExecution
from taro.jobs.runner import RunnerJobInstance
from taro.jobs.sync import ExecutionsLimit
from taro.jobs.track import OutputTracker, MutableTrackedTask, Fields
from taro.test.execution import TestExecution
from taro.util import KVParser, iso_date_time_parser

logger = logging.getLogger(__name__)


# TODO refactor -> extract methods
def run(args):
    if args.dry_run:
        execution = TestExecution(args.dry_run)
    else:
        execution = ProgramExecution(*([args.command] + args.arg), read_output=not args.bypass_output)

    job_id = args.id or " ".join([args.command] + args.arg)
    if args.serial:
        exec_limit = ExecutionsLimit(args.execution_group or job_id, 1)
    elif args.max_executions:
        exec_limit = ExecutionsLimit(args.execution_group or job_id, args.max_executions)
    else:
        exec_limit = None

    output_parsers = []
    for grok_pattern in args.grok_pattern:
        output_parsers.append(Grok(grok_pattern).match)
    if args.kv_filter:
        output_parsers.append(KVParser(post_parsers=[(iso_date_time_parser(Fields.TIMESTAMP.value))]))

    if output_parsers:
        task = MutableTrackedTask(job_id)
        execution.tracking = task
        tracker = OutputTracker(task, output_parsers)
        execution.add_output_observer(tracker)

    job_instance = RunnerJobInstance(
        job_id,
        execution,
        sync.create_composite(executions_limit=exec_limit, no_overlap=args.no_overlap, depends_on=args.depends_on),
        instance_id=args.instance,
        pending_group=args.pending,
        **(dict(args.param) if args.param else dict()))

    warning.register(job_instance, warn_times=args.warn_time, warn_outputs=args.warn_output)

    _set_signal_handlers(job_instance, args.timeout)

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


def _set_signal_handlers(job_instance, timeout_signal):
    term = Term(job_instance)
    signal.signal(signal.SIGTERM, term.terminate)
    signal.signal(signal.SIGINT, term.interrupt)

    if timeout_signal:
        if timeout_signal.isnumeric():
            timeout_signal_number = timeout_signal
        else:
            signal_enum = getattr(signal.Signals, timeout_signal)
            timeout_signal_number = signal_enum.value

        signal.signal(timeout_signal_number, term.timeout)


class Term:

    def __init__(self, job_instance):
        self.job_instance = job_instance

    def terminate(self, _, __):
        logger.warning('event=[terminated_by_signal]')
        self.job_instance.stop()

    def interrupt(self, _, __):
        logger.warning('event=[interrupted_by_keyboard]')
        self.job_instance.interrupted()

    def timeout(self, _, __):
        logger.warning('event=[terminated_by_timeout_signal]')
        self.job_instance.add_warning(Warn('timeout'))
        self.job_instance.stop()


class ProgramExecutionError(SystemExit):
    pass
