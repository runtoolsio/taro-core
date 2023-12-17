from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Sequence, Tuple, Dict, Any

from tarotools.taro import util
from tarotools.taro.util import format_dt_iso, is_empty
from tarotools.taro.util.observer import ObservableNotification

log = logging.getLogger(__name__)


class Tracked(ABC):

    @property
    @abstractmethod
    def first_updated_at(self):
        pass

    @property
    @abstractmethod
    def last_updated_at(self):
        pass

    @property
    @abstractmethod
    def active(self):
        pass


class Trackable:

    def __init__(self, *, parent=None, timestamp_gen=util.utc_now):
        self._parent: Optional[Trackable] = parent
        self._timestamp_gen = timestamp_gen
        self._first_updated_at: Optional[datetime] = None
        self._last_updated_at: Optional[datetime] = None
        self._notification = ObservableNotification[TrackedTaskObserver]()
        self._active = True

    def _updated(self, timestamp):
        timestamp = timestamp or self._timestamp_gen()

        if not self._first_updated_at:
            self._first_updated_at = timestamp
        self._last_updated_at = timestamp

        self._notification.observer_proxy.new_trackable_update()

        if self._parent:
            self._parent._updated(timestamp)

    @staticmethod
    def _update(func):
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            # args[0] should be the instance of TaskTrackerMem
            tracker: Trackable = args[0]
            ts = kwargs.get('timestamp')
            tracker._updated(ts)
            return result

        return wrapper


@dataclass(frozen=True)
class TrackedOperation(Tracked):
    name: Optional[str]
    completed: Optional[float]
    total: Optional[float]
    unit: str = ''
    _first_updated_at: Optional[datetime] = None
    _last_updated_at: Optional[datetime] = None
    _active: bool = False

    @classmethod
    def deserialize(cls, data):
        name = data.get("name")
        completed = data.get("completed", None)
        total = data.get("total", None)
        unit = data.get("unit", '')
        first_update_at = util.parse_datetime(data.get("first_update_at", None))
        last_updated_at = util.parse_datetime(data.get("last_updated_at", None))
        active = data.get("active", False)
        return cls(name, completed, total, unit, first_update_at, last_updated_at, active)

    def serialize(self):
        return {
            'name': self.name,
            'completed': self.completed,
            'total': self.total,
            'unit': self.unit,
            'first_update_at': format_dt_iso(self.first_updated_at),
            'last_updated_at': format_dt_iso(self.last_updated_at),
            'active': self.active,
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

    @property
    def pct_done(self):
        if isinstance(self.completed, (int, float)) and isinstance(self.total, (int, float)):
            return self.completed / self.total
        else:
            return None

    @property
    def finished(self):
        return self.completed and self.total and (self.completed == self.total)

    @property
    def has_progress(self):
        return self.completed or self.total or self.unit

    def _progress_str(self):
        val = f"{self.completed or '?'}"
        if self.total:
            val += f"/{self.total}"
        if self.unit:
            val += f" {self.unit}"
        if pct_done := self.pct_done:
            val += f" ({round(pct_done * 100, 0):.0f}%)"

        return val

    def __str__(self):
        parts = []
        if self.name:
            parts.append(self.name)
        if self.has_progress:
            parts.append(self._progress_str())

        return " ".join(parts)


class OperationTracker(ABC):

    @property
    @abstractmethod
    def tracked_operation(self):
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

    @abstractmethod
    def finished(self):
        pass


class OperationTrackerMem(Trackable, OperationTracker):

    def __init__(self, name):
        super().__init__()
        self._name = name
        self._completed = None
        self._total = None
        self._unit = ''
        self._active = True
        self._finished = False

    @property
    def tracked_operation(self):
        return TrackedOperation(
            self._name,
            self._completed,
            self._total,
            self._unit,
            self._first_updated_at,
            self._last_updated_at,
            self._active)

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
            raise TypeError("Value must be in the format `{number}{unit}` or `{number} {unit}`, but it was: "
                            + str(value))

    @Trackable._update
    def incr_completed(self, completed, *, timestamp=None):
        cnv_completed, unit = self.parse_value(completed)

        if self._completed:
            self._completed += cnv_completed
        else:
            self._completed = cnv_completed

        if unit:
            self._unit = unit

    @Trackable._update
    def set_completed(self, completed, *, timestamp=None):
        self._completed, unit = self.parse_value(completed)
        if unit:
            self._unit = unit

    @Trackable._update
    def set_total(self, total):
        self._total, unit = self.parse_value(total)
        if unit:
            self._unit = unit

    @Trackable._update
    def set_unit(self, unit, *, timestamp=None):
        if not isinstance(unit, str):
            raise TypeError("Unit must be a string")
        self._unit = unit

    @Trackable._update
    def update(self, completed, total=None, unit: str = '', *, increment=False, timestamp=None):
        if completed is None:
            raise ValueError("Value completed must be specified")

        if increment:
            self.incr_completed(completed)
        else:
            self.set_completed(completed)

        if total:
            self.set_total(total)
        if unit:
            self.set_unit(unit)

    @Trackable._update
    def deactivate(self):
        self._active = False

    @Trackable._update
    def finished(self):
        self._finished = True


@dataclass
class Warn:
    """
    This class represents a warning.

    Attributes:
        category (str): Category is used to identify the type of the warning.
        params (Optional[Dict[str, Any]]): Arbitrary parameters describing the warning.
    """
    category: str
    params: Optional[Dict[str, Any]] = None

    @classmethod
    def deserialize(cls, as_dict):
        return cls(as_dict["category"], as_dict.get("params"))

    def serialize(self):
        return {
            "category": self.category,
            "params": self.params,
        }


@dataclass(frozen=True)
class TrackedTask(Tracked):
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
        warnings = [Warn.deserialize(warn) for warn in data.get("warnings", ())]
        first_updated_at = util.parse_datetime(data.get("first_updated_at", None))
        last_updated_at = util.parse_datetime(data.get("last_updated_at", None))
        active = data.get("active")
        return cls(name, current_event, operations, result, subtasks, warnings, first_updated_at, last_updated_at,
                   active)

    def serialize(self, include_empty=True):
        d = {
            'name': self.name,
            'current_event': self.current_event,
            'operations': [op.serialize() for op in self.operations],
            'result': self.result,
            'subtasks': [task.serialize() for task in self.subtasks],
            'warnings': [warn.serialize() for warn in self.warnings],
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

    @property
    @abstractmethod
    def tracked_task(self):
        pass

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


class TaskTrackerMem(Trackable, TaskTracker):

    def __init__(self, name=None):
        super().__init__()
        self._parent: Optional[TaskTrackerMem] = None
        self._name = name
        self._current_event = None
        self._operations = OrderedDict()
        self._subtasks = OrderedDict()
        self._result = None
        self._active = True
        self._notification = ObservableNotification[TrackedTaskObserver]()  # TODO Error hook

    @property
    def tracked_task(self):
        ops = [op.tracked_operation for op in self._operations.values()]
        tasks = [t.tracked_task for t in self._subtasks.values()]
        return TrackedTask(self._name, self._current_event, ops,
                           self._result, tasks, [], self._first_updated_at, self._last_updated_at, self._active)  # TODO

    @Trackable._update
    def event(self, name: str, *, timestamp=None):
        self._current_event = (name, timestamp)

    @Trackable._update
    def reset_current_event(self, *, timestamp=None):
        self._current_event = None

    def operation(self, name, *, timestamp=None):
        op = self._operations.get(name)
        if not op:
            self._operations[name] = (op := OperationTrackerMem(name))
            self._updated(timestamp)

        return op

    def deactivate_finished_operations(self):
        for op in self._operations:
            if op.finished:
                op.active = False

    @Trackable._update
    def result(self, result, *, timestamp=None):
        self._result = result

    def task(self, name, *, timestamp=None):
        task = self._subtasks.get(name)
        if not task:
            self._subtasks[name] = (task := TaskTrackerMem(name))
            task._parent = self
            task._notification.add_observer(self._notification.observer_proxy)
            self._updated(timestamp)

        return task

    @Trackable._update
    def deactivate(self):
        self._active = False

    def deactivate_subtasks(self):
        for subtask in self._subtasks:
            subtask.active = False

    @Trackable._update
    def warning(self, warn):
        pass

    @Trackable._update
    def failure(self, fault_type: str, reason):
        pass


class TrackedTaskObserver(ABC):

    def new_trackable_update(self):
        pass
