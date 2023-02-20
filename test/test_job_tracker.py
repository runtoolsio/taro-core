from datetime import datetime

from taro.jobs.job import OutputTracker
from taro.jobs.track import MutableTrackedTask, Fields
from taro.util import KVParser, iso_date_time_parser


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
