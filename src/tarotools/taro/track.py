import logging
from abc import ABC
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import Enum
from typing import Optional, Sequence, Tuple

from tarotools.taro import util
from tarotools.taro.util import convert_if_number

log = logging.getLogger(__name__)


@dataclass
class Temporal(ABC):

    first_update_at: Optional[datetime] = None
    last_update_at: Optional[datetime] = None


@dataclass
class Activatable(ABC):
    active: bool = False


@dataclass
class Progress(ABC):
    completed: float = 0
    total: float = None
    is_pct: bool = False
    unit: str = ''

    @property
    def pct_done(self):
        if not self.total:
            return 0

        return self.completed / self.total

    @property
    def finished(self):
        return self.completed and self.total and (self.completed == self.total)

    def copy(self):
        return replace(self)


@dataclass
class Operation(Temporal, Activatable):

    name: Optional[str] = None
    progress: Optional[Progress] = field(default_factory=Progress)

    @property
    def finished(self):
        # TODO is_finished field
        return self.progress.finished

    def copy(self):
        return self  # TODO


@dataclass
class TrackedTask(Temporal, Activatable):
    name: str = ''
    events: Sequence[Tuple[str, datetime]] = field(default_factory=list)
    current_event: Optional[Tuple[str, datetime]] = None
    operations: Sequence[Operation] = field(default_factory=list)
    result: str = ''

    def copy(self):
        return type(self)(
            name=self.name,
            events=list(self.events),
            current_event=self.current_event,
            operations=[op.copy() for op in self.operations],
            result=self.result,
            started_at=self.first_update_at,
            updated_at=self.last_update_at,
            ended_at=self.ended_at,
            active=self.active,
        )


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
