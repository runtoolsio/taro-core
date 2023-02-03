import logging
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

log = logging.getLogger(__name__)


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

    @property
    @abstractmethod
    def is_finished(self):
        pass


@dataclass
class ProgressView(Progress):
    completed: Any
    total: Any
    unit: str
    last_update: datetime
    is_finished: bool


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
    def events(self):
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
    def subtasks(self):
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
        self._completed = None
        self._total = None
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

    @property
    def is_finished(self):
        return self.total and (self.completed == self.total)

    def update(self, completed, total=None, unit: str = '', is_increment=False):
        if self.completed and is_increment:
            self._completed += completed  # Must be a number if it's an increment
        else:
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

    def update(self, completed, total=None, unit: str = '', is_increment=False):
        self._progress.update(completed, total, unit, is_increment)

    def __str__(self):
        return f"{self._name}: {self._progress}"


class MutableTrackedTask(TrackedTask):

    def __init__(self, name, max_events=100):
        self._name = name
        self._start_date = None
        self._end_date = None
        self._events = deque(maxlen=max_events)
        self._operations = OrderedDict()
        self._subtasks = OrderedDict()

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
    def events(self):
        return list(self._events)

    def add_event(self, name: str, timestamp=None):
        self._events.append((name, timestamp))  # TODO

    @property
    def last_event(self) -> Optional[str]:
        if not self._events:
            return None
        return self._events[-1]

    def operation(self, name):
        op = self._operations.get(name)
        if not op:
            self._operations[name] = (op := MutableOperation(name))

        return op

    @property
    def operations(self):
        return list(self._operations.values())

    def subtask(self, name):
        task = self._subtasks.get(name)
        if not task:
            self._subtasks[name] = (task := MutableTrackedTask(name))

        return task

    @property
    def subtasks(self):
        return list(self._subtasks.values())

    @property
    def __str__(self):
        statuses = [self.last_event[0] if self.last_event else '']
        statuses += self.operations
        return " | ".join((str(s) for s in statuses))


class Fields(Enum):
    EVENT = 'event'
    TASK = 'task'
    TIMESTAMP = 'timestamp'
    COMPLETED = 'completed'
    INCREMENT = 'increment'
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

        event = match.get(Fields.EVENT.value)
        task = match.get(Fields.TASK.value)
        ts = util.str_to_datetime(match.get(Fields.TIMESTAMP.value))
        completed = match.get(Fields.COMPLETED.value)
        increment = _convert_if_number(match, Fields.INCREMENT)
        total = match.get(Fields.TOTAL.value)
        unit = match.get(Fields.UNIT.value)

        if task:
            rel_task = self.task.subtask(task)
        else:
            rel_task = self.task

        if completed or increment or total or unit:
            rel_task.operation(event).update(completed or increment, total, unit, increment is not None)
        elif event:
            rel_task.add_event(event, ts)


def _convert_if_number(match, field):
    val = match.get(field.value)
    if isinstance(val, (int, float)) or not val:
        return val

    if '.' in (dec := val.replace(',', '.')):
        try:
            return float(dec)
        except ValueError:
            pass

    try:
        return int(val)
    except ValueError:
        pass

    return val


