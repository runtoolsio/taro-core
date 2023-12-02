from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Sequence, Tuple

from tarotools.taro import util
from tarotools.taro.instance import Warn
from tarotools.taro.util import format_dt_iso, is_empty

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Temporal(ABC):
    first_updated_at: Optional[datetime]
    last_updated_at: Optional[datetime]


@dataclass
class MutableTemporal:
    _first_updated_at: Optional[datetime] = None
    _last_updated_at: Optional[datetime] = None


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
    _active: bool

    @classmethod
    def deserialize(cls, data):
        first_update_at = util.parse_datetime(data.get("first_update_at", None))
        last_updated_at = util.parse_datetime(data.get("last_updated_at", None))
        name = data.get("name")
        if progress_data := data.get("progress", None):
            progress = TrackedProgress.deserialize(progress_data)
        else:
            progress = None
        active = data.get("active", False)
        return cls(first_update_at, last_updated_at, name, progress, active)

    def serialize(self):
        return {
            'first_update_at': format_dt_iso(self.first_updated_at),
            'last_updated_at': format_dt_iso(self.last_updated_at),
            'name': self.name,
            'progress': self.progress.serialize(),
            'active': self.active,
        }

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


class Operation(MutableTemporal, OperationTracker):

    def __init__(self, name):
        super().__init__()
        self._name = name
        self._progress = None
        self._active = True
        self._finished = False

    @property
    def operation(self):
        return TrackedOperation(self._name, self._progress, self._first_updated_at, self._last_updated_at, self._active)

    @property
    def progress_tracker(self):
        return self._progress

    def finished(self):
        self._finished = True


@dataclass(frozen=True)
class TrackedTask(Temporal, Activatable):
    # TODO: failure
    name: str
    current_event: Optional[Tuple[str, datetime]]
    operations: Sequence[TrackedOperation]
    result: str
    subtasks: Sequence[TrackedTask]
    warnings: Sequence[Warn]
    _active: bool

    @classmethod
    def deserialize(cls, data):
        first_updated_at = util.parse_datetime(data.get("first_updated_at", None))
        last_updated_at = util.parse_datetime(data.get("last_updated_at", None))
        name = data.get("name")
        current_event = data.get("current_event")
        operations = [TrackedOperation.deserialize(op) for op in data.get("operations", ())]
        result = data.get("result")
        subtasks = [TrackedTask.deserialize(task) for task in data.get("subtasks", ())]
        warnings = [Warn.deserialize(warn) for warn in data.get("warnings", ())]
        active = data.get("active")
        return cls(first_updated_at, last_updated_at, name, current_event, operations, result, subtasks, warnings, active)

    def serialize(self, include_empty=True):
        d = {
            'first_update_at': format_dt_iso(self.first_updated_at),
            'last_updated_at': format_dt_iso(self.last_updated_at),
            'name': self.name,
            'current_event': self.current_event,
            'operations': [op.serialize() for op in self.operations],
            'result': self.result,
            'subtasks': [task.serialize() for task in self.subtasks],
            'warnings': [warn.serialize() for warn in self.warnings],
            'active': self.active,
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

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

    @abstractmethod
    def event(self, event, timestamp=None):
        pass

    @abstractmethod
    def operation(self, op_name):
        pass

    @abstractmethod
    def result(self, result):
        pass

    @abstractmethod
    def task(self, task_name):
        pass

    @abstractmethod
    def warning(self, warn):
        pass

    @abstractmethod
    def failure(self, fault_type: str, reason):
        pass


class Task(MutableTemporal, TaskTracker):

    def __init__(self, name=None):
        super().__init__()
        self._name = name
        self._current_event = None
        self._operations = OrderedDict()
        self._subtasks = OrderedDict()
        self._result = None
        self._active = True

    def event(self, name: str, timestamp=None):
        self._current_event = (name, timestamp)

    def reset_current_event(self):
        self._current_event = None

    def operation(self, name):
        op = self._operations.get(name)
        if not op:
            self._operations[name] = (op := Operation(name))

        return op

    def deactivate_finished_operations(self):
        for op in self._operations:
            if op.finished:
                op.active = False

    def result(self, result):
        self._result = result

    def task(self, name):
        task = self._subtasks.get(name)
        if not task:
            self._subtasks[name] = (task := Task(name))

        return task

    def deactivate_subtasks(self):
        for subtask in self._subtasks:
            subtask.active = False

    def warning(self, warn):
        pass

    def failure(self, fault_type: str, reason):
        pass
