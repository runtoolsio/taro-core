import logging
from abc import ABC
from abc import abstractmethod
from collections import deque, OrderedDict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Any, Sequence, Tuple

from taro import util
from taro.util import TimePeriod, datetime_to_str

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
    def pct_done(self):
        if isinstance(self.completed, (int, float)) and isinstance(self.total, (int, float)):
            return self.completed / self.total
        else:
            return None

    @property
    def is_finished(self):
        return self.completed and self.total and (self.completed == self.total)

    def copy(self):
        return ProgressInfo(self.completed, self.total, self.unit, self.last_update)

    def to_dict(self):
        return {
            'completed': self.completed,
            'total': self.total,
            'unit': self.unit,
            'last_update': datetime_to_str(self.last_update),
            'pct_done': self.pct_done,
            'is_finished': self.is_finished
        }

    def __str__(self):
        val = f"{self.completed or '?'}/{self.total or '?'}"
        if self.unit:
            val += f" {self.unit}"
        if pct_done := self.pct_done:
            val += f" ({round(pct_done * 100, 0):.0f}%)"

        return val


@dataclass(frozen=True)
class ProgressInfo(Progress):

    _completed: Any
    _total: Any
    _unit: str = ''
    _last_update: datetime = None

    @classmethod
    def from_dict(cls, data):
        completed = data.get("completed", None)
        total = data.get("total", None)
        unit = data.get("unit", '')
        last_update = util.str_to_datetime(data.get("last_update", None))
        return cls(completed, total, unit, last_update)

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


class Operation(TimePeriod):

    @property
    @abstractmethod
    def name(self):
        pass

    @property
    @abstractmethod
    def progress(self):
        pass

    def copy(self):
        return OperationInfo(self.name, self.progress.copy(), self.start_date, self.end_date)

    def to_dict(self):
        return {
            'name': self.name,
            'progress': self.progress.to_dict(),
            'start_date': datetime_to_str(self.start_date),
            'end_date': datetime_to_str(self.end_date)
        }

    def __str__(self):
        return f"{self.name}: {self.progress}"


@dataclass(frozen=True)
class OperationInfo(Operation):
    _name: str
    _progress: Progress
    _start_date: datetime
    _end_date: datetime

    @classmethod
    def from_dict(cls, data):
        name = data.get("name", None)
        if progress_data := data.get("progress", None):
            progress = ProgressInfo.from_dict(progress_data)
        else:
            progress = None
        start_date = util.str_to_datetime(data.get("start_date", None))
        end_date = util.str_to_datetime(data.get("end_date", None))
        return cls(name, progress, start_date, end_date)

    @property
    def name(self):
        return self._name

    @property
    def progress(self):
        return self._progress

    @property
    def start_date(self):
        return self._start_date

    @property
    def end_date(self):
        return self._end_date


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

    def copy(self):
        return TrackedTaskInfo(
            self.name,
            self.events,
            [op.copy() for op in self.operations],
            [task.copy() for task in self.subtasks],
            self.start_date,
            self.end_date)

    def to_dict(self):
        return {
            'name': self.name,
            'events': self.events,
            'operations': [op.to_dict() for op in self.operations],
            'subtasks': [task.to_dict() for task in self.subtasks],
            'start_date': datetime_to_str(self.start_date),
            'end_date': datetime_to_str(self.end_date),
        }


@dataclass(frozen=True)
class TrackedTaskInfo(TrackedTask):
    _name: str
    _events: Sequence[Tuple[str, datetime]]
    _operations: Sequence[Operation]
    _subtasks: Sequence[TrackedTask]
    _start_date: datetime
    _end_date: datetime

    @classmethod
    def from_dict(cls, data):
        name = data.get("name", None)
        events = data.get("events", ())
        operations = [OperationInfo.from_dict(op) for op in data.get("operations", ())]
        subtasks = [TrackedTaskInfo.from_dict(task) for task in data.get("subtasks", ())]
        start_date = util.str_to_datetime(data.get("start_date", None))
        end_date = util.str_to_datetime(data.get("end_date", None))
        return cls(name, events, operations, subtasks, start_date, end_date)

    @property
    def name(self):
        return self._name

    @property
    def events(self):
        return self._events

    @property
    def last_event(self):
        return self._events[-1] if self._events else None

    @property
    def operations(self):
        return self._operations

    @property
    def subtasks(self):
        return self._subtasks

    @property
    def start_date(self):
        return self._start_date

    @property
    def end_date(self):
        return self._end_date


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

    def __str__(self):
        if self.last_event:
            statuses = [f"{util.format_dt_ms_local_tz(self.last_event[1])} {self.last_event[0]} "]
        else:
            statuses = []
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


DEFAULT_PATTERN = ''
