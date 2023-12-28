from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from collections import OrderedDict, namedtuple
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Sequence

from tarotools.taro import util
from tarotools.taro.util import format_dt_iso, is_empty
from tarotools.taro.util.observer import ObservableNotification

log = logging.getLogger(__name__)


class Tracked(ABC):

    @property
    @abstractmethod
    def created_at(self):
        pass

    @property
    @abstractmethod
    def updated_at(self):
        pass

    @property
    @abstractmethod
    def finished(self):
        pass


class Trackable:

    def __init__(self, parent=None, *, created_at=None, timestamp_gen=util.utc_now):
        self._parent: Optional[Trackable] = parent
        self._timestamp_gen = timestamp_gen
        self._created_at: Optional[datetime] = created_at or timestamp_gen()
        self._updated_at: Optional[datetime] = None
        self._notification = ObservableNotification[TrackedTaskObserver]()  # TODO Error hook
        self._active = True

    def _updated(self, timestamp):
        timestamp = timestamp or self._timestamp_gen()
        self._updated_at = timestamp
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
    _created_at: Optional[datetime] = None
    _updated_at: Optional[datetime] = None
    _active: bool = False

    @classmethod
    def deserialize(cls, data):
        name = data.get("name")
        completed = data.get("completed", None)
        total = data.get("total", None)
        unit = data.get("unit", '')
        created_at = util.parse_datetime(data.get("created_at", None))
        updated_at = util.parse_datetime(data.get("updated_at", None))
        active = data.get("active", False)
        return cls(name, completed, total, unit, created_at, updated_at, active)

    def serialize(self):
        return {
            'name': self.name,
            'completed': self.completed,
            'total': self.total,
            'unit': self.unit,
            'created_at': format_dt_iso(self.created_at),
            'updated_at': format_dt_iso(self.updated_at),
            'active': self.finished,
        }

    @property
    def created_at(self):
        return self._created_at

    @property
    def updated_at(self):
        return self._updated_at

    @property
    def finished(self):
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
    def incr_completed(self, completed, *, timestamp=None):
        pass

    @abstractmethod
    def set_completed(self, completed, *, timestamp=None):
        pass

    @abstractmethod
    def set_total(self, total, *, timestamp=None):
        pass

    @abstractmethod
    def set_unit(self, unit, *, timestamp=None):
        pass

    @abstractmethod
    def update(self, completed, total, unit=None, *, timestamp=None):
        pass

    @abstractmethod
    def finished(self, *, timestamp=None):
        pass


class OperationTrackerMem(Trackable, OperationTracker):

    def __init__(self, name, parent, *, created_at=None, timestamp_gen=util.utc_now):
        super().__init__(parent, created_at=created_at, timestamp_gen=timestamp_gen)
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
            self._created_at,
            self._updated_at,
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
    def set_total(self, total, *, timestamp=None):
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
            self.incr_completed(completed, timestamp=timestamp)
        else:
            self.set_completed(completed, timestamp=timestamp)

        if total:
            self.set_total(total, timestamp=timestamp)
        if unit:
            self.set_unit(unit, timestamp=timestamp)

    @Trackable._update
    def deactivate(self):
        self._active = False

    @Trackable._update
    def finished(self, *, timestamp=None):
        self._finished = True


Event = namedtuple('Event', ['text', 'timestamp'])


@dataclass(frozen=True)
class TrackedTask(Tracked):
    # TODO: failure
    name: str
    current_event: Optional[Event]
    operations: Sequence[TrackedOperation]
    result: str
    subtasks: Sequence[TrackedTask]
    warnings: Sequence[Event]
    _created_at: Optional[datetime]
    _updated_at: Optional[datetime]
    _finished: bool

    @classmethod
    def deserialize(cls, data):
        name = data.get("name")
        current_event = Event(*data.get("current_event")) if data.get("current_event") else None
        operations = [TrackedOperation.deserialize(op) for op in data.get("operations", ())]
        result = data.get("result")
        subtasks = [TrackedTask.deserialize(task) for task in data.get("subtasks", ())]
        warnings = [Event(*warn) for warn in data.get("warnings", ())]
        created_at = util.parse_datetime(data.get("created_at", None))
        updated_at = util.parse_datetime(data.get("updated_at", None))
        finished = data.get("finished")
        return cls(name, current_event, operations, result, subtasks, warnings, created_at, updated_at, finished)

    def serialize(self, include_empty=True):
        d = {
            'name': self.name,
            'current_event': tuple(self.current_event) if self.current_event else None,
            'operations': [op.serialize() for op in self.operations],
            'result': self.result,
            'subtasks': [task.serialize() for task in self.subtasks],
            'warnings': [tuple(warn) for warn in self.warnings],
            'created_at': format_dt_iso(self.created_at),
            'updated_at': format_dt_iso(self.updated_at),
            'finished': self.finished,
        }
        if include_empty:
            return d
        else:
            return {k: v for k, v in d.items() if not is_empty(v)}

    def find_subtask(self, name):
        for subtask in self.subtasks:
            if subtask.name == name:
                return subtask
        return None

    def find_operation(self, name):
        for operation in self.operations:
            if operation.name == name:
                return operation
        return None

    @property
    def created_at(self):
        return self._created_at

    @property
    def updated_at(self):
        return self._updated_at

    @property
    def finished(self):
        return self._finished

    def __str__(self):
        parts = []

        if self.finished:
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
            statuses += [op for op in self.operations if op.finished]
            if statuses:
                parts.append(" | ".join((str(s) for s in statuses)))

        if self.subtasks:
            if parts:
                parts.append('/')
            parts.append(' / '.join(str(task) for task in self.subtasks if task.finished))

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
    def finished(self, result=None, *, timestamp=None):
        pass

    @property
    @abstractmethod
    def is_finished(self):
        pass

    @abstractmethod
    def subtask(self, task_name):
        pass

    @property
    @abstractmethod
    def subtasks(self):
        pass

    @abstractmethod
    def warning(self, warn):
        pass


class TaskTrackerMem(Trackable, TaskTracker):

    def __init__(self, name=None, parent=None, *, created_at=None, timestamp_gen=util.utc_now):
        super().__init__(parent, created_at=created_at, timestamp_gen=timestamp_gen)
        self._name = name
        self._current_event = None
        self._operations = OrderedDict()
        self._subtasks = OrderedDict()
        self._warnings = []
        self._result = None
        self._active = True

    @property
    def tracked_task(self):
        ops = [op.tracked_operation for op in self._operations.values()]
        tasks = [t.tracked_task for t in self._subtasks.values()]
        return TrackedTask(self._name, self._current_event, ops, self._result, tasks, self._warnings,
                           self._created_at, self._updated_at, self._active)

    @Trackable._update
    def event(self, name: str, *, timestamp=None):
        self._current_event = (name, timestamp or self._timestamp_gen())

    def operation(self, name, *, timestamp=None):
        op = self._operations.get(name)
        if not op:
            self._operations[name] = (op := OperationTrackerMem(name, self, created_at=timestamp))
            self._updated(timestamp)

        return op

    def deactivate_finished_operations(self):
        for op in self._operations:
            if op.finished:
                op.finished = False

    @Trackable._update
    def finished(self, result=None, *, timestamp=None):
        self._result = result

    @property
    def is_finished(self):
        return self._active

    def subtask(self, name, *, timestamp=None):
        task = self._subtasks.get(name)
        if not task:
            self._subtasks[name] = \
                (task := TaskTrackerMem(name, self, created_at=timestamp, timestamp_gen=self._timestamp_gen))
            task._parent = self
            task._notification.add_observer(self._notification.observer_proxy)
            self._updated(timestamp)

        return task

    @property
    def subtasks(self):
        return list(self._subtasks.values())

    @Trackable._update
    def deactivate(self):
        self._active = False

    def deactivate_subtasks(self):
        for subtask in self._subtasks:
            subtask.finished = False

    @Trackable._update
    def warning(self, warn, *, timestamp=None):
        self._warnings.append(Event(warn, timestamp or self._timestamp_gen()))



class TrackedTaskObserver(ABC):

    def new_trackable_update(self):
        pass
