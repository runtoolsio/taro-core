import re

from tarotools.taro.track import TrackedOperation, Task, TrackedProgress
from tarotools.taro.util import parse_datetime


def test_add_event():
    task = Task('task')
    task.event('e1')
    task.event('e2')

    assert task.tracked_task.current_event[0] == 'e2'


def test_operation_updates():
    task = Task('task')
    task.operation('op1').progress.update(1, 10, 'items')

    op1 = task.tracked_task.operations[0]
    assert op1.name == 'op1'
    assert op1.progress.completed == 1
    assert op1.progress.total == 10
    assert op1.progress.unit == 'items'


def test_operation_incr_update():
    task = Task('task')
    task.operation('op1').update(1, increment=True)

    op1 = task.tracked_task.operations[0]
    assert op1.name == 'op1'
    assert op1.progress.completed == 1
    assert op1.progress.total is None
    assert op1.progress.unit == ''

    task.operation('op1').update(3, 5, increment=True)
    task.operation('op2').update(0, 10)
    assert op1.progress.completed == 4
    assert op1.progress.total == 5

    op2 = task.tracked_task.operations[1]
    assert op2.name == 'op2'
    assert op2.progress.total == 10


def test_subtask():
    task = Task('main')
    task.task('s1').add_event('e1')
    task.task('s1').operation('01').update(2)
    task.task(2).add_event('e2')

    tracked_task = task.tracked_task
    assert tracked_task.current_event is None
    assert tracked_task.subtasks[0].current_event[0] == 'e1'
    assert tracked_task.subtasks[0].operations[0].name == '01'
    assert tracked_task.subtasks[1].current_event[0] == 'e2'


def test_operation_str():
    empty_op = TrackedOperation(None, None, 'name', None, True)
    assert str(empty_op) == 'name'

    assert len(str(TrackedOperation(None, None, None, TrackedProgress(25, 100, 'files'), True)).split()) > 1
    assert re.search(r"name .*", str(TrackedOperation(None, None, None, TrackedProgress(25, 100, 'files'), True)))


def test_progress_str():
    progress = TrackedProgress(25, 100, 'files')
    assert str(progress) == '25/100 files (25%)'

    progress = TrackedProgress(None, 100, 'files')
    assert str(progress) == '?/100 files'

    progress = TrackedProgress(20, None, 'files')
    assert str(progress) == '20 files'


def test_task_str():
    task = Task('task1')
    assert str(task) == 'task1:'
    task.event('e1', parse_datetime('2023-01-01T00:00:00'))
    assert str(task) == 'task1: e1'
    task.event('e2', parse_datetime('2023-01-01T01:00:00'))
    assert str(task) == 'task1: e2'
    task.operation('downloading')
    task.reset_current_event()
    assert str(task) == 'task1: downloading'
    task.operation('downloading').update(None, None, 'files')
    assert str(task) == 'task1: downloading ? files'
    task.operation('uploading')
    assert str(task) == 'task1: downloading ? files | uploading'
    task.operation('downloading').active = False
    assert str(task) == 'task1: uploading'
    task.event('e3', parse_datetime('2023-01-01T02:00:00'))
    assert str(task) == 'task1: e3 | uploading'
    task.task('sub-zero').operation('freezing')
    assert str(task) == 'task1: e3 | uploading / sub-zero: freezing'
    task.active = False
    assert str(task) == 'sub-zero: freezing'
    task.task('scorpion').add_event('burning', parse_datetime('2023-01-01T05:00:00'))
    assert str(task) == 'sub-zero: freezing / scorpion: burning'
    task.task('scorpion').result = 'fatality'
    assert str(task) == 'sub-zero: freezing / scorpion: fatality'
