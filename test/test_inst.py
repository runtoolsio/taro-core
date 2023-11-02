from tarotools.taro import TerminationStatus
from tarotools.taro.test.inst import TestJobInstance


def test_notification_priority():
    notified = []

    def observer(priority):
        def appender(*_):
            notified.append(priority)
        return appender

    i = TestJobInstance()
    i.add_transition_callback(observer(2), 2)
    i.add_transition_callback(observer(4), 4)
    i.add_transition_callback(observer(1), 1)
    i.add_transition_callback(observer(3), 3)

    i.change_phase(TerminationStatus.RUNNING)

    assert notified == [1, 2, 3, 4]
