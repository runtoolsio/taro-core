from tarotools.taro import ExecutionState
from tarotools.taro.jobs.featurize import FeaturedContextBuilder
from tarotools.taro.test.inst import TestJobInstanceManager, TestJobInstance
from tarotools.taro.test.observer import TestStateObserver


class FeatHelper:

    def __init__(self, factory):
        self.factory = factory
        self.feature = None
        self.opened = False
        self.closed = False

    def __call__(self):
        self.feature = self.factory()
        return self.feature

    def open(self, _):
        self.opened = True

    def close(self, _):
        self.closed = True


def test_instance_manager():
    helper = FeatHelper(TestJobInstanceManager)
    ctx = FeaturedContextBuilder().add_instance_manager(helper, open_hook=helper.open, close_hook=helper.close).build()
    inst_manager = helper.feature
    assert not helper.opened

    with ctx:
        assert helper.opened
        assert not helper.closed

        inst = TestJobInstance()
        ctx.add(inst)
        assert inst_manager.instances[0] == inst

    assert not helper.closed  # Closed only after the last instance is terminated
    assert inst_manager.instances

    inst.change_state(ExecutionState.COMPLETED)
    assert not inst_manager.instances
    assert helper.closed


def test_state_observer():
    helper = FeatHelper(TestStateObserver)
    ctx = (FeaturedContextBuilder()
           .add_state_observer(helper, open_hook=helper.open, close_hook=helper.close, priority=111)
           .build())
    observer = helper.feature

    with ctx:
        assert helper.opened

        inst = TestJobInstance()
        ctx.add(inst)
        assert inst.state_notification.observers[0] == (111, observer)

    assert inst.state_notification.observers

    inst.change_state(ExecutionState.COMPLETED)
    assert not inst.state_notification.observers
    assert helper.closed
