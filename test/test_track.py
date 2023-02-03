from datetime import datetime

from taro.jobs.track import MutableTrackedTask, GrokTrackingParser


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
    task.operation('op1').update(1, is_increment=True)

    op1 = task.operations[0]
    assert op1.name == 'op1'
    assert op1.progress.completed == 1
    assert op1.progress.total is None
    assert op1.progress.unit == ''

    task.operation('op1').update(3, 5, is_increment=True)
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

    assert task.last_event is None
    assert task.subtasks[0].last_event[0] == 'e1'
    assert task.subtasks[0].operations[0].name == '01'
    assert task.subtasks[1].last_event[0] == 'e2'


def test_grok_event():
    task = MutableTrackedTask('task')
    grok = GrokTrackingParser(task, "event=\\[%{WORD:event}\\]")

    grok.new_output('no events here')
    assert task.last_event is None

    grok.new_output('event=[eventim_apollo] we have first event here')
    assert task.last_event[0] == 'eventim_apollo'

    grok.new_output('second event follows event=[event_horizon]')
    assert task.last_event[0] == 'event_horizon'


def test_grok_timestamps():
    task = MutableTrackedTask('task')
    grok = GrokTrackingParser(task, "%{TIMESTAMP_ISO8601:timestamp} event=\\[%{WORD:event}\\]")

    grok.new_output('2020-10-01 10:30:30 event=[e1]')
    assert task.last_event[1] == datetime.strptime('2020-10-01 10:30:30', "%Y-%m-%d %H:%M:%S")

    grok.new_output('2020-10-01T10:30:30.543 event=[e1]')
    assert task.last_event[1] == datetime.strptime('2020-10-01 10:30:30.543', "%Y-%m-%d %H:%M:%S.%f")


def test_grok_optional():
    task = MutableTrackedTask('task')
    grok = GrokTrackingParser(task, "(event=\\[%{WORD:event}\\])? (count=\\[%{NUMBER:completed}\\])?")

    grok.new_output("event=[downloaded] count=[10] total=[100] unit=[files]")
    assert task.operations[0].name == 'downloaded'
    assert task.operations[0].progress.completed == 10
