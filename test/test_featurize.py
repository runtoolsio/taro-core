from dataclasses import dataclass

from tarotools.taro.jobs.featurize import FeaturedContextBuilder
from tarotools.taro.jobs.instance import JobInstanceManager
from tarotools.taro.run import RunState
from tarotools.taro.test.instance import FakeJobInstanceBuilder, FakeJobInstance
from tarotools.taro.test.observer import TestTransitionObserver, TestOutputObserver


@dataclass
class TestJobInstanceManager(JobInstanceManager):

    def __init__(self):
        self.instances = []

    def register_instance(self, job_instance):
        self.instances.append(job_instance)

    def unregister_instance(self, job_instance):
        self.instances.remove(job_instance)


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


class TestEnv:

    def __init__(self, *, transient=True):
        self.instance_manager = FeatHelper(TestJobInstanceManager)
        self.instance_manager_volatile = FeatHelper(TestJobInstanceManager)
        self.transition_observer = FeatHelper(TestTransitionObserver)
        self.output_observer = FeatHelper(TestOutputObserver)
        self.all_features = (self.instance_manager, self.transition_observer, self.output_observer)
        self.ctx = (FeaturedContextBuilder(transient=transient)
                    .add_instance_manager(self.instance_manager,
                                          open_hook=self.instance_manager.open,
                                          close_hook=self.instance_manager.close)
                    .add_instance_manager(self.instance_manager_volatile, unregister_after_termination=True)
                    .add_transition_observer(self.transition_observer,
                                             open_hook=self.transition_observer.open,
                                             close_hook=self.transition_observer.close,
                                             priority=111)
                    .add_output_observer(self.output_observer,
                                         open_hook=self.output_observer.open,
                                         close_hook=self.output_observer.close,
                                         priority=112)
                    .build())

    def not_opened(self):
        return not any(f.opened for f in self.all_features)

    def opened(self):
        return all(f.opened for f in self.all_features) and not any(f.closed for f in self.all_features)

    def closed(self):
        return all(f.closed for f in self.all_features)

    def instance_registered(self, job_instance: FakeJobInstance):
        added_to_manager = job_instance in self.instance_manager.feature.instances
        state_observer_registered = (
                (111, self.transition_observer.feature) in job_instance.prioritized_transition_observers)
        # TODO output observer
        return added_to_manager and state_observer_registered

    def instance_contained(self, job_instance: FakeJobInstance):
        return bool(self.ctx.get_instance(job_instance.instance_id)) and job_instance in self.ctx.instances


def test_instance_management():
    env = TestEnv()

    assert env.not_opened()
    with env.ctx:
        assert env.opened()
        assert not env.closed()

        inst = FakeJobInstanceBuilder().add_phase('EXEC', RunState.EXECUTING).build()
        inst.phaser.next_phase()
        env.ctx.add(inst)
        assert env.instance_registered(inst)
        assert env.instance_contained(inst)

    assert not env.closed()  # Closed only after the last instance is terminated
    assert env.instance_registered(inst)
    assert env.instance_contained(inst)  # Instance removed only after is terminated

    inst.phaser.next_phase()  # Terminated
    assert env.transition_observer.feature.last_state == RunState.ENDED
    assert env.closed()
    assert not env.instance_registered(inst)
    assert not env.instance_contained(inst)


def test_removed_when_terminated_before_closed():
    env = TestEnv()

    with env.ctx:
        inst = FakeJobInstanceBuilder().add_phase('EXEC', RunState.EXECUTING).build()
        env.ctx.add(inst)
        inst.phaser.next_phase()
        assert env.instance_registered(inst)
        assert env.instance_contained(inst)

        inst.phaser.next_phase()  # Terminated
        assert not env.instance_registered(inst)
        assert not env.instance_contained(inst)


def test_keep_removed():
    env = TestEnv(transient=False)
    with env.ctx:
        inst = FakeJobInstanceBuilder().add_phase('EXEC', RunState.EXECUTING).build()
        inst.phaser.next_phase()
        env.ctx.add(inst)
        assert env.instance_registered(inst)
        assert env.instance_contained(inst)
        assert env.instance_manager_volatile.feature.instances

        inst.phaser.next_phase()  # Terminated
        assert not env.instance_registered(inst)
        assert env.instance_contained(inst)
        assert not env.instance_manager_volatile.feature.instances  # Set to unregister terminated

    assert env.closed()
    assert env.instance_contained(inst)
