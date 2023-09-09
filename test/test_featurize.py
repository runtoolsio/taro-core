from tarotools.taro.jobs.featurize import FeaturedContextBuilder
from tarotools.taro.test.inst import TestJobInstanceManager, TestJobInstance


class CreatedHolder:

    def __init__(self, factory):
        self.factory = factory
        self.created = None

    def __call__(self):
        self.created = self.factory()
        return self.created


def test_instance_manager():
    holder = CreatedHolder(TestJobInstanceManager)
    ctx = FeaturedContextBuilder().add_instance_manager(holder, open_hook=lambda feat: feat.open()).build()
    inst_manager = holder.created

    assert not inst_manager.opened
    assert not inst_manager.closed

    with ctx:
        assert inst_manager.opened
        assert not inst_manager.closed

        inst = TestJobInstance()
        ctx.add(inst)
        assert inst_manager.instances[0] == inst
