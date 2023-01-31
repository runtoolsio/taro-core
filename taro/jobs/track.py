from abc import ABC
from abc import abstractmethod
from collections import deque, OrderedDict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Any

from pygrok import Grok

from taro import JobInfo, util
from taro.jobs.execution import ExecutionOutputObserver
from taro.jobs.job import JobOutputObserver
from taro.util import TimePeriod


class Progress(ABC):

    @property
    @abstractmethod
    def completed(self):
        pass

    @property
    @abstractmethod
    def total(self):
        pass

    @property
    @abstractmethod
    def unit(self):
        pass

    @property
    @abstractmethod
    def last_update(self):
        pass


@dataclass
class ProgressView(Progress):

    completed: Any
    total: Any
    unit: str
    last_update: datetime


class Operation(TimePeriod):

    @property
    @abstractmethod
    def name(self):
        pass

    @property
    @abstractmethod
    def progress(self):
        pass


class TrackedTask(TimePeriod):

    @property
    @abstractmethod
    def name(self):
        pass

    @property
    @abstractmethod
    def last_event(self):
        pass

    @property
    @abstractmethod
    def operations(self):
        pass

    @property
    @abstractmethod
    def status(self):
        pass


class MutableTimePeriod(TimePeriod):

    def __init__(self):
        self._start_date = None
        self._end_date = None

    @property
    def start_date(self):
        return self._start_date

    @property
    def end_date(self):
        return self._end_date


class MutableProgress(Progress):

    def __init__(self):
        self._completed = 0
        self._total = 0
        self._unit = ''
        self._last_update = None

    @property
    def completed(self):
        return self._completed

    @property
    def total(self):
        return self._total

    @property
    def unit(self):
        return self._unit

    @property
    def last_update(self):
        return self._last_update

    def update(self, completed: int, total: int = 0, unit: str = ''):
        self._completed = completed
        if total:
            self._total = total
        if unit:
            self._unit = unit
        self._last_update = None  # TODO TBD

    def __str__(self):
        if self._total:
            return f"{self._completed}/{self._total} {self._unit}"
        else:
            return f"{self._completed} {self._unit}"


class MutableOperation(Operation):
    def __init__(self, name):
        self._name = name
        self._start_date = None
        self._end_date = None
        self._progress = MutableProgress()

    @property
    def name(self):
        return self._name

    @property
    def start_date(self):
        return self._start_date

    @property
    def end_date(self):
        return self._end_date

    @property
    def progress(self):
        return self._progress

    def update(self, completed: int, total: int = 0, unit: str = ''):
        self._progress.update(completed, total, unit)

    def __str__(self):
        return f"{self._name}: {self._progress}"


class MutableTrackedTask(TrackedTask):
    def __init__(self, name, max_events=100):
        self._name = name
        self._start_date = None
        self._end_date = None
        self._events = deque(maxlen=max_events)
        self._operations = OrderedDict()

    @property
    def name(self):
        return self._name

    @property
    def start_date(self):
        return self._start_date

    @property
    def end_date(self):
        return self._end_date

    def add_event(self, name: str, timestamp=None):
        self._events.appendleft((name, timestamp))  # TODO

    @property
    def last_event(self) -> Optional[str]:
        if not self._events:
            return None
        return self._events[0]

    def update_operation(self, name, completed, total, unit):
        op = self._operations.get(name)
        if not op:
            self._operations[name] = (op := MutableOperation(name))
        op.update(completed, total, unit)

    @property
    def operations(self):
        return self._operations

    @property
    def status(self):
        statuses = [self.last_event[0] if self.last_event else '']
        statuses += self.operations.values()
        return " | ".join((str(s) for s in statuses))


class Fields(Enum):
    EVENT = 'event'
    TIMESTAMP = 'timestamp'
    COMPLETED = 'completed'
    TOTAL = 'total'
    UNIT = 'unit'


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
        unit = match.get(Fields.UNIT.value)
        if completed or total or unit:
            self.task.update_operation(event, completed, total, unit)


def _str_to_dt(timestamp):
    # TODO support more formats
    return util.dt_from_utc_str(timestamp)
