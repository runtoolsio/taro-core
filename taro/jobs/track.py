from enum import Enum

from pygrok import Grok

from taro import JobInfo
from taro.jobs.execution import ExecutionOutputObserver
from taro.jobs.job import JobOutputObserver


class Fields(Enum):
    EVENT = 'event'


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

        event = match.get(Fields.EVENT.value)
        self.task.add_event(event)