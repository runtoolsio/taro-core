import re
from datetime import datetime

from taro.jobs.track import MutableTrackedTask, OperationInfo, ProgressInfo, OutputTracker, Fields
from taro.util import parse_datetime, KVParser, iso_date_time_parser


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
    task = MutableTrackedTask('task1')
    assert str(task) == 'task1:'
    task.add_event('e1', parse_datetime('2023-01-01T00:00:00'))
    assert str(task) == 'task1: 00:00:00 e1'
    task.add_event('e2', parse_datetime('2023-01-01T01:00:00'))
    assert str(task) == 'task1: 01:00:00 e2'
    task.operation('downloading')
    task.reset_current_event()
    assert str(task) == 'task1: downloading'
    task.operation('downloading').update(None, None, 'files')
    assert str(task) == 'task1: downloading ?/? files'
    task.operation('uploading')
    assert str(task) == 'task1: downloading ?/? files | uploading'
    task.operation('downloading').active = False
    assert str(task) == 'task1: uploading'
    task.add_event('e3', parse_datetime('2023-01-01T02:00:00'))
    assert str(task) == 'task1: 02:00:00 e3 | uploading'
    task.subtask('sub-zero').operation('freezing')
    assert str(task) == 'task1: 02:00:00 e3 | uploading / sub-zero: freezing'
    task.active = False
    assert str(task) == 'sub-zero: freezing'
    task.subtask('scorpion').add_event('burning', parse_datetime('2023-01-01T05:00:00'))
    assert str(task) == 'sub-zero: freezing / scorpion: 05:00:00 burning'


def test_parse_event():
    task = MutableTrackedTask('task')
    tracker = OutputTracker(task, [KVParser()])

    tracker.new_output('no events here')
    assert task.current_event is None

    tracker.new_output('event=[eventim_apollo] we have first event here')
    assert task.current_event[0] == 'eventim_apollo'

    tracker.new_output('second follows: event=[event_horizon]')
    assert task.current_event[0] == 'event_horizon'


def test_parse_timestamps():
    task = MutableTrackedTask('task')
    tracker = OutputTracker(task, [KVParser(post_parsers=[(iso_date_time_parser(Fields.TIMESTAMP.value))])])

    tracker.new_output('2020-10-01 10:30:30 event=[e1]')
    assert task.current_event[1] == datetime.strptime('2020-10-01 10:30:30', "%Y-%m-%d %H:%M:%S")

    tracker.new_output('2020-10-01T10:30:30.543 event=[e1]')
    assert task.current_event[1] == datetime.strptime('2020-10-01 10:30:30.543', "%Y-%m-%d %H:%M:%S.%f")


def test_parse_progress():
    task = MutableTrackedTask('task')
    tracker = OutputTracker(task, [KVParser(aliases={'count': 'completed'})])

    tracker.new_output("event=[downloaded] count=[10] total=[100] unit=[files]")
    assert task.operations[0].name == 'downloaded'
    assert task.operations[0].progress.completed == 10
    assert task.operations[0].progress.total == 100
    assert task.operations[0].progress.unit == 'files'


def test_multiple_parsers_and_tasks():
    def fake_parser(_):
        return {'timestamp': '2020-10-01 10:30:30'}

    task = MutableTrackedTask('main')
    # Test multiple parsers can be used together to parse the same input
    tracker = OutputTracker(task, [KVParser(value_split=":"), KVParser(field_split="&"), fake_parser])

    tracker.new_output('task:task1')
    tracker.new_output('?time=2.3&task=task2&event=e1')
    assert task.subtasks[0].name == 'task1'
    assert task.subtasks[1].name == 'task2'
    assert task.subtasks[1].current_event[0] == 'e1'
    assert str(task.subtasks[1].current_event[1]) == '2020-10-01 10:30:30'
    assert not task.events


def test_operation_resets_last_event():
    task = MutableTrackedTask()
    tracker = OutputTracker(task, [KVParser()])
    tracker.new_output("event=[upload]")
    tracker.new_output("event=[decoding] completed=[10]")

    assert task.current_event is None
