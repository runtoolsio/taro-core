from enum import Enum

from pygrok import Grok

from taro import JobInfo, util
from taro.jobs.execution import ExecutionOutputObserver
from taro.jobs.job import JobOutputObserver


class Fields(Enum):
    EVENT = 'event'
    TIMESTAMP = 'timestamp'
    COMPLETED = 'completed'
    TOTAL = 'total'


class GrokTrackingParser(ExecutionOutputObserver, JobOutputObserver):

    def __init__(self, task, pattern):
        self.task = task
        self.grok = Grok(pattern)

    def execution_output_update(self, output, is_error: bool):
        self.new_output(output)

    def job_output_update(self, job_info: JobInfo, output, is_error):
        self.new_output(output)

    def new_output(self, output):
        match = self.grok.match(output)
        if not match:
            return

        ts = _str_to_dt(match.get(Fields.TIMESTAMP.value))

        event = match.get(Fields.EVENT.value)
        if event:
            self.task.add_event(event, ts)

        completed = match.get(Fields.COMPLETED.value)
        total = match.get(Fields.TOTAL.value)
        if completed or total:
            self.task.update_operation(event, completed, total)


def _str_to_dt(timestamp):
    # TODO support more formats
    return util.dt_from_utc_str(timestamp)