from taro.jobs.track import MutableTrackedTask


def test_add_event():
    task = MutableTrackedTask('task1')
    task.add_event('e1')
    task.add_event('e2')

    assert [event for event, ts in task.events] == ['e1', 'e2']


def test_operation_updates():
    task = MutableTrackedTask('task1')
    task.update_operation('op1', 1, 10, 'items')

    op1 = task.operations[0]
    assert op1.name == 'op1'
    assert op1.progress.completed == 1
    assert op1.progress.total == 10
    assert op1.progress.unit == 'items'


def test_operation_incr_update():
    task = MutableTrackedTask('task1')
    task.update_operation('op1', 1, is_increment=True)

    op1 = task.operations[0]
    assert op1.name == 'op1'
    assert op1.progress.completed == 1
    assert op1.progress.total is None
    assert op1.progress.unit == ''

    task.update_operation('op1', 3, 5, is_increment=True)
    task.update_operation('op2', 0, 10)
    assert op1.progress.completed == 4
    assert op1.progress.total == 5

    op2 = task.operations[1]
    assert op2.name == 'op2'
    assert op2.progress.total == 10

