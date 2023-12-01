from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections import deque, OrderedDict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Sequence, Tuple

from tarotools.taro import util
from tarotools.taro.instance import Warn
from tarotools.taro.util import format_dt_iso, convert_if_number, is_empty

log = logging.getLogger(__name__)


class Temporal(ABC):

    @property
    @abstractmethod
    def first_updated_at(self):
        pass

    @property
    @abstractmethod
    def last_updated_at(self):
        pass


@dataclass
class MutableTemporal(Temporal):
    _first_updated_at: Optional[datetime] = None
    _last_updated_at: Optional[datetime] = None

    @property
    def first_updated_at(self) -> Optional[datetime]:
        return self._first_updated_at

    @first_updated_at.setter
    def first_updated_at(self, started_at: datetime) -> None:
        self._first_updated_at = started_at

    @property
    def last_updated_at(self) -> Optional[datetime]:
        return self._last_updated_at

    @last_updated_at.setter
    def last_updated_at(self, updated_at: datetime) -> None:
        self._last_updated_at = updated_at


class Activatable(ABC):

    @property
    @abstractmethod
    def active(self):
        pass


@dataclass(frozen=True)
class TrackedProgress:
    _completed: Optional[float]
    _total: Optional[float]
    _unit: str = ''

    @classmethod
    def deserialize(cls, data):
        completed = data.get("completed", None)
        total = data.get("total", None)
        unit = data.get("unit", '')
        return cls(completed, total, unit)

    def serialize(self):
        return {
            'completed': self.completed,
            'total': self.total,
            'unit': self.unit,
        }

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
    def pct_done(self):
        if isinstance(self.completed, (int, float)) and isinstance(self.total, (int, float)):
            return self.completed / self.total
        else:
            return None

    @property
    def finished(self):
        return self.completed and self.total and (self.completed == self.total)

    def __str__(self):
        val = f"{self.completed or '?'}"
        if self.total:
            val += f"/{self.total}"
        if self.unit:
            val += f" {self.unit}"
        if pct_done := self.pct_done:
            val += f" ({round(pct_done * 100, 0):.0f}%)"

        return val


class ProgressTracker(ABC):

    @property
    @abstractmethod
    def progress(self):
        pass

    @abstractmethod
    def incr_completed(self, completed):
        pass

    @abstractmethod
    def set_completed(self, completed):
        pass

    @abstractmethod
    def set_total(self, total):
        pass

    @abstractmethod
    def set_unit(self, unit):
        pass

    @abstractmethod
    def update(self, completed, total, unit=None):
        pass


class Progress(ProgressTracker):

    def __init__(self):
        self._completed = None
        self._total = None
        self._unit = ''

    @property
    def progress(self):
        return TrackedProgress(self._completed, self._total, self._unit)

    def parse_value(self, value):
        # Check if value is a string and extract number and unit
        if isinstance(value, str):
            match = re.match(r"(\d+(\.\d+)?)(\s*)(\w+)?", value)
            if match:
                number = float(match.group(1))
                unit = match.group(4) if match.group(4) else ''
                return number, unit
            else:
                raise ValueError("String format is not correct. Expected format: {number}{unit} or {number} {unit}")
        elif isinstance(value, (float, int)):
            return float(value), self._unit
        else:
            raise TypeError("Value must be a float, int, or a string in the format {number}{unit} or {number} {unit}")

    def incr_completed(self, completed):
        cnv_completed, unit = self.parse_value(completed)

        if self._completed:
            self._completed += cnv_completed
        else:
            self._completed = cnv_completed

        if unit:
            self._unit = unit

    def set_completed(self, completed):
        self._completed, unit = self.parse_value(completed)
        if unit:
            self._unit = unit

    def set_total(self, total):
        self._total, unit = self.parse_value(total)
        if unit:
            self._unit = unit

    def set_unit(self, unit):
        if not isinstance(unit, str):
            raise TypeError("Unit must be a string")
        self._unit = unit

    def update(self, completed, total=None, unit: str = ''):
        self.set_completed(completed)
        if total:
            self.set_total(total)
        if unit:
            self.set_unit(unit)


@dataclass(frozen=True)
class TrackedOperation(Temporal, Activatable):
    name: Optional[str]
    progress: Optional[TrackedProgress]
    _first_updated_at: Optional[datetime]
    _last_updated_at: Optional[datetime]
    _active: bool

    @classmethod
    def deserialize(cls, data):
        name = data.get("name")
        if progress_data := data.get("progress", None):
            progress = TrackedProgress.deserialize(progress_data)
        else:
            progress = None
        first_update_at = util.parse_datetime(data.get("first_update_at", None))
        last_updated_at = util.parse_datetime(data.get("last_updated_at", None))
        active = data.get("active", False)
        return cls(name, progress, first_update_at, last_updated_at, active)

    def serialize(self):
        return {
            'first_update_at': format_dt_iso(self.first_updated_at),
            'last_updated_at': format_dt_iso(self.last_updated_at),
            'active': self.active,
            'name': self.name,
            'progress': self.progress.serialize(),
        }

    @property
    def first_updated_at(self):
        return self._first_updated_at

    @property
    def last_updated_at(self):
        return self._last_updated_at

    @property
    def active(self):
        return self._active

    def __str__(self):
        parts = []
        if self.name:
            parts.append(self.name)
        if self.progress:
            parts.append(str(self.progress))

        return " ".join(parts)


class OperationTracker(ABC):

    @property
    @abstractmethod
    def operation(self):
        pass

    @property
    @abstractmethod
    def progress_tracker(self):
        pass

    @abstractmethod
    def finished(self):
        pass


@dataclass(frozen=True)
class TrackedTask(Temporal, Activatable):
    # TODO: failure
    name: str
    current_event: Optional[Tuple[str, datetime]]
    operations: Sequence[TrackedOperation]
    result: str
    subtasks: Sequence[TrackedTask]
    warnings: Sequence[Warn]
    _first_updated_at: Optional[datetime]
    _last_updated_at: Optional[datetime]
    _active: bool

    @classmethod
    def deserialize(cls, data):
        name = data.get("name")
        current_event = data.get("current_event")
        operations = [TrackedOperation.deserialize(op) for op in data.get("operations", ())]
        result = data.get("result")
        subtasks = [TrackedTask.deserialize(task) for task in data.get("subtasks", ())]
        started_at = util.parse_datetime(data.get("first_update_at", None))
        updated_at = util.parse_datetime(data.get("last_updated_at", None))
        active = data.get("active")
        return cls(name, current_event, operations, subtasks, result, started_at, updated_at, active)

    def serialize(self, include_empty=True):
        d = {
            'name': self.name,
            'current_event': self.current_event,
            'operations': [op.serialize() for op in self.operations],
            'result': self.result,
            'subtasks': [task.serialize() for task in self.subtasks],
            'first_update_at': format_dt_iso(self.first_updated_at),
            'last_updated_at': format_dt_iso(self.last_updated_at),
            'active': self.active,
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    @property
    def first_updated_at(self):
        return self._first_updated_at

    @property
    def last_updated_at(self):
        return self._last_updated_at

    @property
    def active(self):
        return self._active

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


class TaskTracker(ABC):

    def event(self, event):
        pass

    def operation(self, op_name):
        pass

    def result(self, result):
        pass

    def task(self, task_name):
        pass

    def warning(self, warn):
        pass

    def failure(self, fault_type: str, reason):
        pass


class MutableOperation(MutableTemporal, TrackedOperation):

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
            self._progress = ProgressTracker()

        if not self.first_updated_at:
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
        rel_task.last_update_at = fields.get(Fields.TIMESTAMP)
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
