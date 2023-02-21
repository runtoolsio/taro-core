import re

from taro.jobs.track import MutableTrackedTask, OperationInfo, ProgressInfo, TrackedTaskInfo
from taro.util import parse_datetime


def test_add_event():
    task = MutableTrackedTask('task')
    task.add_event('e1')
    task.add_event('e2')

    assert [event for event, ts in task.events] == ['e1', 'e2']


def test_operation_updates():
    task = MutableTrackedTask('task')
    task.operation('op1').update(1, 10, 'items')

    op1 = task.operations[0]
    assert op1.name == 'op1'
    assert op1.progress.completed == 1
    assert op1.progress.total == 10
    assert op1.progress.unit == 'items'


def test_operation_incr_update():
    task = MutableTrackedTask('task')
    task.operation('op1').update(1, increment=True)

    op1 = task.operations[0]
    assert op1.name == 'op1'
    assert op1.progress.completed == 1
    assert op1.progress.total is None
    assert op1.progress.unit == ''

    task.operation('op1').update(3, 5, increment=True)
    task.operation('op2').update(0, 10)
    assert op1.progress.completed == 4
    assert op1.progress.total == 5

    op2 = task.operations[1]
    assert op2.name == 'op2'
    assert op2.progress.total == 10


def test_subtask():
    task = MutableTrackedTask('main')
    task.subtask('s1').add_event('e1')
    task.subtask('s1').operation('01').update(2)
    task.subtask(2).add_event('e2')

    assert task.current_event is None
    assert task.subtasks[0].current_event[0] == 'e1'
    assert task.subtasks[0].operations[0].name == '01'
    assert task.subtasks[1].current_event[0] == 'e2'


def test_operation_str():
    empty_op = OperationInfo('name', None, None, None, True)
    assert str(empty_op) == 'name'

    assert len(str(OperationInfo(None, ProgressInfo(25, 100, 'files'), None, None, True)).split()) > 1
    assert re.search(r"name .*", str(OperationInfo('name', ProgressInfo(25, 100, 'files'), None, None, True)))


def test_progress_str():
    progress = ProgressInfo(25, 100, 'files')
    assert str(progress) == '25/100 files (25%)'

    progress = ProgressInfo(None, 100, 'files')
    assert str(progress) == '?/100 files'

    progress = ProgressInfo(20, None, 'files')
    assert str(progress) == '20/? files'


def test_task_str():
    events = [('e1', parse_datetime('2023-01-01T00:00:00')), ('e2', parse_datetime('2023-01-01T01:00:00'))]
    task = TrackedTaskInfo('task1', events, events[1], (), (), None, None, True)

    assert str(task) == 'task1: 01:00:00 e2'
