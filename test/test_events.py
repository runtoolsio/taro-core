
from tarotools.taro.jobs.events import TransitionDispatcher, OutputDispatcher
from tarotools.taro.jobs.instance import JobInstanceMetadata
from tarotools.taro.listening import InstanceTransitionReceiver, InstanceOutputReceiver
from tarotools.taro.run import PhaseRun, RunState, PhaseMetadata
from tarotools.taro.test.instance import ended_run
from tarotools.taro.test.observer import GenericObserver
from tarotools.taro.util import utc_now


def test_state_dispatching():
    dispatcher = TransitionDispatcher()
    receiver = InstanceTransitionReceiver()
    observer = GenericObserver()
    receiver.add_observer_transition(observer)
    receiver.start()

    test_run = ended_run('j1')
    prev = PhaseRun('prev', RunState.PENDING, utc_now(), utc_now())
    new = PhaseRun('next', RunState.EXECUTING, utc_now(), None)
    try:
        dispatcher.new_phase(test_run, prev, new, 2)
    finally:
        dispatcher.close()
        receiver.close()

    instance_meta, prev_run, new_run, ordinal = observer.updates.get(timeout=2)[1]
    assert instance_meta.metadata.job_id == 'j1'
    assert prev_run == prev
    assert new_run == new
    assert ordinal == 2


def test_output_dispatching():
    dispatcher = OutputDispatcher()
    receiver = InstanceOutputReceiver()
    observer = GenericObserver()
    receiver.add_observer_output(observer)
    receiver.start()
    instance_meta = JobInstanceMetadata('j1', 'r1', 'i1', {}, {})
    phase = PhaseMetadata("Bar in Pai", RunState.EXECUTING, {})
    try:
        dispatcher.new_instance_output(instance_meta, phase, "Happy Mushrooms", True)
    finally:
        dispatcher.close()
        receiver.close()

    o_instance_meta, o_phase, new_output, is_error = observer.updates.get(timeout=2)[1]
    assert o_instance_meta == instance_meta
    assert o_phase == phase
    assert new_output == "Happy Mushrooms"
    assert is_error
