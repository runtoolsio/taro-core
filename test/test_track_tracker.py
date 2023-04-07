from datetime import datetime

from taro.jobs.track import MutableTrackedTask, OutputTracker, Fields
from taro.util import KVParser, iso_date_time_parser


def test_parse_event():
    task = MutableTrackedTask('task')
    tracker = OutputTracker(task, [KVParser()])

    tracker.new_output('no events here')
    assert task.current_event is None
    assert not task.events

    tracker.new_output('non_existing_field=[huh]')
    assert task.current_event is None
    assert not task.events

    tracker.new_output('event=[eventim_apollo] we have first event here')
    assert task.current_event[0] == 'eventim_apollo'

    tracker.new_output('second follows: event=[event_horizon]')
    assert task.current_event[0] == 'event_horizon'


def test_operation_without_name():
    task = MutableTrackedTask('task')
    tracker = OutputTracker(task, [KVParser()])

    tracker.new_output('operation without name completed=[5]')
    assert task.current_event is None
    assert task.operations[0].progress.completed == 5


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


def test_event_deactivate_completed_operation():
    task = MutableTrackedTask()
    tracker = OutputTracker(task, [KVParser()])

    tracker.new_output("event=[encoding] completed=[10] total=[10]")
    assert task.operations[0].active

    tracker.new_output("event=[new_event]")
    assert not task.operations[0].active


def test_subtask_deactivate_current_task():
    task = MutableTrackedTask()
    tracker = OutputTracker(task, [KVParser()])

    tracker.new_output("event=[event_in_main_task]")
    assert task.active

    tracker.new_output("event=[event_in_subtask] task=[subtask1]")
    assert not task.active
    assert task.subtasks[0].active

    tracker.new_output("event=[another_event_in_main_task]")
    assert task.active
    assert not task.subtasks[0].active


def test_task_started_and_update_on_event():
    task = MutableTrackedTask()
    tracker = OutputTracker(task, [KVParser(), iso_date_time_parser(Fields.TIMESTAMP.value)])
    tracker.new_output('2020-10-01 10:30:30 event=[e1]')
    tracker.new_output('2020-10-01 11:45:00 event=[e2]')
    assert task.started_at == datetime(2020, 10, 1, 10, 30, 30)
    assert task.updated_at == datetime(2020, 10, 1, 11, 45, 0)


def test_task_started_and_updated_on_operation():
    task = MutableTrackedTask()
    tracker = OutputTracker(task, [KVParser(), iso_date_time_parser(Fields.TIMESTAMP.value)])
    tracker.new_output('2020-10-01 14:40:00 event=[op1] total=[200]')
    tracker.new_output('2020-10-01 15:30:30 event=[op1] total=[400]')
    started_ts = datetime(2020, 10, 1, 14, 40, 0)
    updated_ts = datetime(2020, 10, 1, 15, 30, 30)
    assert task.started_at == started_ts
    assert task.operation('op1').started_at == started_ts
    assert task.updated_at == updated_ts
    assert task.operation('op1').updated_at == updated_ts


def test_op_end_date():
    task = MutableTrackedTask()
    tracker = OutputTracker(task, [KVParser(), iso_date_time_parser(Fields.TIMESTAMP.value)])
    tracker.new_output('2020-10-01 14:40:00 event=[op1] completed=[5] total=[10]')
    assert task.operation('op1').ended_at is None

    tracker.new_output('2020-10-01 15:30:30 event=[op1] completed=[10] total=[10]')
    assert task.operation('op1').ended_at == datetime(2020, 10, 1, 15, 30, 30)


def test_subtask_started_and_updated_set():
    task = MutableTrackedTask()
    tracker = OutputTracker(task, [KVParser(), iso_date_time_parser(Fields.TIMESTAMP.value)])
    tracker.new_output('2020-10-01 12:30:00 task=[t1]')
    tracker.new_output('2020-10-01 13:50:00 task=[t1] event=[e1]')

    started_ts = datetime(2020, 10, 1, 12, 30, 0)
    updated_ts = datetime(2020, 10, 1, 13, 50, 0)
    assert task.subtask('t1').started_at == started_ts
    assert task.subtask('t1').updated_at == updated_ts
    assert task.started_at is None  # TODO should this be set too?


def test_parse_timestamps():
    task = MutableTrackedTask('task')
    tracker = OutputTracker(task, [KVParser()])

    tracker.new_output('2020-10-01 10:30:30 event=[e1]')
    tracker.new_output('result=[res]')
    assert task.result == 'res'
