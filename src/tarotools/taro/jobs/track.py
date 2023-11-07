import logging
from abc import ABC, abstractmethod
from collections import deque, OrderedDict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Any, Sequence, Tuple

from tarotools.taro import util
from tarotools.taro.util import format_dt_iso, parse_datetime, convert_if_number, is_empty

log = logging.getLogger(__name__)


class Temporal(ABC):

    @property
    @abstractmethod
    def started_at(self):
        pass

    @property
    @abstractmethod
    def updated_at(self):
        pass

    @property
    @abstractmethod
    def ended_at(self):
        pass

    @property
    def finished(self):
        return self.started_at and self.ended_at


class MutableTemporal(Temporal):

    def __init__(self):
        self._started_at = None
        self._updated_at = None
        self._ended_at = None

    @property
    def started_at(self):
        return self._started_at

    @started_at.setter
    def started_at(self, started_at):
        self._started_at = started_at

    @property
    def updated_at(self):
        return self._updated_at

    @updated_at.setter
    def updated_at(self, updated_at):
        self._updated_at = updated_at

    @property
    def ended_at(self):
        return self._ended_at

    @ended_at.setter
    def ended_at(self, ended_at):
        self._ended_at = ended_at

    def __hash__(self):
        return hash((self.started_at, self.updated_at, self.ended_at))

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return (self.started_at, self.updated_at, self.ended_at) == (other.started_at, other.updated_at, other.ended_at)


class Activatable(ABC):

    @property
    @abstractmethod
    def active(self):
        pass


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
    def last_updated_at(self):
        pass

    @property
    def pct_done(self):
        if isinstance(self.completed, (int, float)) and isinstance(self.total, (int, float)):
            return self.completed / self.total
        else:
            return None

    @property
    def finished(self):
        return self.completed and self.total and (self.completed == self.total)

    def copy(self):
        return ProgressInfo(self.completed, self.total, self.unit, self.last_updated_at)

    def to_dict(self, include_empty=True):
        d = {
            'completed': self.completed,
            'total': self.total,
            'unit': self.unit,
            'last_updated_at': format_dt_iso(self.last_updated_at),
            'pct_done': self.pct_done,
            'finished': self.finished
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    def __str__(self):
        val = f"{self.completed or '?'}"
        if self.total:
            val += f"/{self.total}"
        if self.unit:
            val += f" {self.unit}"
        if pct_done := self.pct_done:
            val += f" ({round(pct_done * 100, 0):.0f}%)"

        return val

    def __hash__(self):
        return hash((self.completed, self.total, self.unit, self.last_updated_at))

    def __eq__(self, other):
        if not isinstance(other, Progress):
            return NotImplemented
        return (self.completed, self.total, self.unit, self.last_updated_at) == \
            (other.completed, other.total, other.unit, other.last_updated_at)


@dataclass(frozen=True, eq=False)
class ProgressInfo(Progress):
    _completed: Any
    _total: Any
    _unit: str = ''
    _last_updated_at: datetime = None

    @classmethod
    def from_dict(cls, data):
        completed = data.get("completed", None)
        total = data.get("total", None)
        unit = data.get("unit", '')
        last_updated_at = util.parse_datetime(data.get("last_updated_at", None))
        return cls(completed, total, unit, last_updated_at)

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
    def last_updated_at(self):
        return self._last_updated_at


class Operation(Temporal, Activatable):

    @property
    @abstractmethod
    def name(self):
        pass

    @property
    @abstractmethod
    def progress(self):
        pass

    @property
    def finished(self):
        return super().finished or self.progress.finished

    def copy(self):
        return OperationInfo(
            self.name, self.progress.copy(), self.started_at, self.updated_at, self.ended_at, self.active)

    def to_dict(self, include_empty=True):
        d = {
            'name': self.name,
            'progress': self.progress.serialize(include_empty),
            'started_at': format_dt_iso(self.started_at),
            'updated_at': format_dt_iso(self.updated_at),
            'ended_at': format_dt_iso(self.ended_at),
            'active': self.active
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    def __str__(self):
        parts = []
        if self.name:
            parts.append(self.name)
        if self.progress:
            parts.append(str(self.progress))

        return " ".join(parts)

    def __hash__(self):
        return hash((self.name, self.progress, self.started_at, self.updated_at, self.ended_at, self.active))

    def __eq__(self, other):
        if not isinstance(other, Operation):
            return NotImplemented
        return (self.name, self.progress, self.started_at, self.updated_at, self.ended_at, self.active) == \
            (other.name, other.progress, other.started_at, other.updated_at, other.ended_at, other.active)


@dataclass(frozen=True, eq=False)
class OperationInfo(Operation):
    _name: Optional[str]
    _progress: Optional[Progress]
    _started_at: Optional[datetime]
    _updated_at: Optional[datetime]
    _ended_at: Optional[datetime]
    _active: bool

    @classmethod
    def from_dict(cls, data):
        name = data.get("name")
        if progress_data := data.get("progress", None):
            progress = ProgressInfo.from_dict(progress_data)
        else:
            progress = None
        started_at = util.parse_datetime(data.get("started_at", None))
        updated_at = util.parse_datetime(data.get("updated_at", None))
        ended_at = util.parse_datetime(data.get("ended_at", None))
        active = data.get("active", False)
        return cls(name, progress, started_at, updated_at, ended_at, active)

    @property
    def name(self):
        return self._name

    @property
    def progress(self):
        return self._progress

    @property
    def started_at(self):
        return self._started_at

    @property
    def updated_at(self):
        return self._updated_at

    @property
    def ended_at(self):
        return self._ended_at

    @property
    def active(self):
        return self._active


class TrackedTask(Temporal, Activatable):

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
    def current_event(self):
        pass

    @property
    @abstractmethod
    def operations(self):
        pass

    @property
    @abstractmethod
    def subtasks(self):
        pass

    @property
    @abstractmethod
    def result(self):
        pass

    def copy(self):
        return TrackedTaskInfo(
            self.name,
            self.events,
            self.current_event,
            [op.copy() for op in self.operations],
            [task.copy() for task in self.subtasks],
            self.result,
            self.started_at,
            self.updated_at,
            self.ended_at,
            self.active,
        )

    def to_dict(self, include_empty=True):
        d = {
            'name': self.name,
            'events': [(event, format_dt_iso(ts)) for event, ts in self.events],
            'operations': [op.serialize(include_empty) for op in self.operations],
            'subtasks': [task.serialize(include_empty) for task in self.subtasks],
            'started_at': format_dt_iso(self.started_at),
            'updated_at': format_dt_iso(self.updated_at),
            'ended_at': format_dt_iso(self.ended_at),
            'active': self.active,
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    def __str__(self):
        parts = []

        if self.active:
            if self.name:
                parts.append(f"{self.name}:")

            if self.result:
                parts.append(self.result)
                return " ".join(parts)

            statuses = []
            if self.current_event:
                if self.current_event[1] and False:  # TODO configurable
                    ts = util.format_time_local_tz(self.current_event[1], include_ms=False)
                    event_str = f"{ts} {self.current_event[0]}"
                else:
                    event_str = self.current_event[0]
                statuses.append(event_str)
            statuses += [op for op in self.operations if op.active]
            if statuses:
                parts.append(" | ".join((str(s) for s in statuses)))

        if self.subtasks:
            if parts:
                parts.append('/')
            parts.append(' / '.join(str(task) for task in self.subtasks if task.active))

        return " ".join(parts)

    def __hash__(self):
        return hash((self.name, tuple(self.events), self.current_event, tuple(self.operations), tuple(self.subtasks),
                     self.result, self.started_at, self.updated_at, self.ended_at, self.active))

    def __eq__(self, other):
        if not isinstance(other, TrackedTask):
            return NotImplemented
        return (
            self.name, tuple(self.events), self.current_event, tuple(self.operations), tuple(self.subtasks),
            self.result,
            self.started_at, self.updated_at, self.ended_at, self.active) == (
            other.name, tuple(other.events), other.current_event, tuple(other.operations), tuple(other.subtasks),
            other.result, other.started_at, other.updated_at, other.ended_at, other.active)


@dataclass(frozen=True, eq=False)
class TrackedTaskInfo(TrackedTask):
    _name: str
    _events: Sequence[Tuple[str, datetime]]
    _current_event: Optional[Tuple[str, datetime]]
    _operations: Sequence[Operation]
    _subtasks: Sequence[TrackedTask]
    _result: str
    _started_at: Optional[datetime]
    _updated_at: Optional[datetime]
    _ended_at: Optional[datetime]
    _active: bool

    @classmethod
    def from_dict(cls, data):
        name = data.get("name")
        events = [(event, parse_datetime(ts)) for event, ts in data.get("events", ())]
        current_event = data.get("current_event")
        operations = [OperationInfo.from_dict(op) for op in data.get("operations", ())]
        subtasks = [TrackedTaskInfo.from_dict(task) for task in data.get("subtasks", ())]
        result = data.get("result")
        started_at = util.parse_datetime(data.get("started_at", None))
        updated_at = util.parse_datetime(data.get("updated_at", None))
        ended_at = util.parse_datetime(data.get("ended_at", None))
        active = data.get("active")
        return cls(name, events, current_event, operations, subtasks, result, started_at, updated_at, ended_at, active)

    @property
    def name(self):
        return self._name

    @property
    def events(self):
        return self._events

    @property
    def current_event(self):
        return self._current_event

    @property
    def operations(self):
        return self._operations

    @property
    def subtasks(self):
        return self._subtasks

    @property
    def result(self):
        return self._result

    @property
    def started_at(self):
        return self._started_at

    @property
    def updated_at(self):
        return self._updated_at

    @property
    def ended_at(self):
        return self._ended_at

    @property
    def active(self):
        return self._active


class MutableProgress(Progress):

    def __init__(self):
        self._completed = None
        self._total = None
        self._unit = ''
        self._last_updated_at = None

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
    def last_updated_at(self):
        return self._last_updated_at

    def update(self, completed, total=None, unit: str = '', timestamp=None, *, increment=False):
        if self.completed and increment:
            self._completed += completed  # Must be a number if it's an increment
        else:
            self._completed = completed

        if total:
            self._total = total
        if unit:
            self._unit = unit
        self._last_updated_at = timestamp


class MutableOperation(MutableTemporal, Operation):

    def __init__(self, name):
        super().__init__()
        self._name = name
        self._progress = None
        self._active = True

    @property
    def name(self):
        return self._name

    @property
    def progress(self):
        return self._progress

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, active):
        self._active = active

    def update(self, completed, total=None, unit: str = '', timestamp=None, *, increment=False):
        if not self._progress:
            self._progress = MutableProgress()

        if not self.started_at:
            self.started_at = timestamp
        self.updated_at = timestamp

        self._progress.update(completed, total, unit, timestamp, increment=increment)

        if not self.ended_at and self.progress.finished:
            self._ended_at = timestamp


class MutableTrackedTask(MutableTemporal, TrackedTask):

    def __init__(self, name=None, max_events=1000):
        super().__init__()
        self._name = name
        self._max_events = max_events
        self._events = deque(maxlen=max_events)
        self._current_event = None
        self._operations = OrderedDict()
        self._subtasks = OrderedDict()
        self._result = None
        self._active = True

    @property
    def name(self):
        return self._name

    @property
    def events(self):
        return list(self._events)

    def add_event(self, name: str, timestamp=None):
        event = (name, timestamp)
        self._events.append(event)
        self._current_event = event

    @property
    def current_event(self) -> Optional[str]:
        return self._current_event

    def reset_current_event(self):
        self._current_event = None

    def operation(self, name):
        op = self._operations.get(name)
        if not op:
            self._operations[name] = (op := MutableOperation(name))

        return op

    @property
    def operations(self):
        return list(self._operations.values())

    def has_operation(self, name):
        if not self.operations:
            return False

        return any(1 for op in self.operations if op.name != name)

    def deactivate_finished_operations(self):
        for op in self.operations:
            if op.finished:
                op.active = False

    def subtask(self, name):
        task = self._subtasks.get(name)
        if not task:
            self._subtasks[name] = (task := MutableTrackedTask(name, max_events=self._max_events))

        return task

    @property
    def subtasks(self):
        return list(self._subtasks.values())

    def deactivate_subtasks(self):
        for subtask in self.subtasks:
            subtask.active = False

    @property
    def result(self):
        return self._result

    @result.setter
    def result(self, result):
        self._result = result

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, active):
        self._active = active


class Fields(Enum):
    EVENT = 'event'
    TASK = 'task'
    TIMESTAMP = 'timestamp'
    COMPLETED = 'completed'
    INCREMENT = 'increment'
    TOTAL = 'total'
    UNIT = 'unit'
    RESULT = 'result'


DEFAULT_PATTERN = ''


def field_conversion(parsed):
    converted = {
        Fields.EVENT: parsed.get(Fields.EVENT.value),
        Fields.TASK: parsed.get(Fields.TASK.value),
        Fields.TIMESTAMP: util.parse_datetime(parsed.get(Fields.TIMESTAMP.value)),
        Fields.COMPLETED: convert_if_number(parsed.get(Fields.COMPLETED.value)),
        Fields.INCREMENT: convert_if_number(parsed.get(Fields.INCREMENT.value)),
        Fields.TOTAL: convert_if_number(parsed.get(Fields.TOTAL.value)),
        Fields.UNIT: parsed.get(Fields.UNIT.value),
        Fields.RESULT: parsed.get(Fields.RESULT.value),
    }

    return {key: value for key, value in converted.items() if value is not None}


class OutputTracker:

    def __init__(self, mutable_task, parsers, conversion=field_conversion):
        self.task = mutable_task
        self.parsers = parsers
        self.conversion = conversion

    def __call__(self, output, is_error=False):
        self.new_output(output, is_error)

    def new_output(self, output, is_error=False):
        parsed = {}
        for parser in self.parsers:
            if p := parser(output):
                parsed.update(p)

        if not parsed:
            return

        fields = self.conversion(parsed)
        if not fields:
            return

        task = self._update_task(fields)
        if not self._update_operation(task, fields):
            task.add_event(fields.get(Fields.EVENT), fields.get(Fields.TIMESTAMP))

    def _update_task(self, fields):
        task = fields.get(Fields.TASK)
        if task:
            rel_task = self.task.subtask(task)
            self.task.active = False
        else:
            rel_task = self.task

        if not rel_task.phase_started_at:
            rel_task.phase_started_at = fields.get(Fields.TIMESTAMP)
        rel_task.updated_at = fields.get(Fields.TIMESTAMP)
        rel_task.active = True
        rel_task.deactivate_subtasks()
        rel_task.deactivate_finished_operations()
        result = fields.get(Fields.RESULT)
        if result:
            rel_task.result = result

        return rel_task

    def _update_operation(self, task, fields):
        op_name = fields.get(Fields.EVENT)
        ts = fields.get(Fields.TIMESTAMP)
        completed = fields.get(Fields.COMPLETED)
        increment = fields.get(Fields.INCREMENT)
        total = fields.get(Fields.TOTAL)
        unit = fields.get(Fields.UNIT)

        if not completed and not increment and not total and not unit:
            return False

        if not task.has_operation(op_name):
            task.reset_current_event()

        task.operation(op_name).update(completed or increment, total, unit, ts, increment=increment is not None)
        return True
