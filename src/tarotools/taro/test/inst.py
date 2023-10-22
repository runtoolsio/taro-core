from collections import Counter
from dataclasses import dataclass

from tarotools.taro import JobInst, JobInstanceID, ExecutionError, TerminationStatus
from tarotools.taro import util
from tarotools.taro.jobs.instance import JobInstanceMetadata, JobInstanceManager, \
    InstancePhaseNotification, RunnableJobInstance, MutableInstanceLifecycle, InstancePhase
from tarotools.taro.jobs.track import MutableTrackedTask
from tarotools.taro.util.observer import DEFAULT_OBSERVER_PRIORITY


def i(job_id, instance_id=None, params=None, user_params=None, lifecycle=None, tracking=None, status=None,
      error_output=None, warnings=None, exec_error=None):
    meta = JobInstanceMetadata(JobInstanceID(job_id, instance_id or util.unique_timestamp_hex()), params, user_params)
    return JobInst(meta, lifecycle, tracking, status, error_output, warnings, exec_error)


class TestJobInstance(RunnableJobInstance):

    def __init__(self, job_id="", instance_id="", phase=TerminationStatus.CREATED):
        self._id = JobInstanceID(job_id, instance_id)
        self._metadata = JobInstanceMetadata(self._id, (), {})
        self._lifecycle = MutableInstanceLifecycle()
        self._tracking = MutableTrackedTask()
        self._status = None
        self._last_output = None
        self._warnings = Counter()
        self._exec_error = None
        self._error_output = None
        self.state_notification = InstancePhaseNotification()
        self.ran = False
        self.released = False
        self.stopped = False
        self.interrupted = False

        self.change_phase(InstancePhase.CREATED)
        self.change_phase(phase)

    def change_phase(self, phase):
        self.lifecycle.new_phase(phase)
        self.state_notification.notify_all_phase_changed(self.create_snapshot())

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        self._id = value

    @property
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, value):
        self._metadata = value

    def run(self):
        self.ran = True

    def release(self):
        self.released = True

    @property
    def lifecycle(self):
        return self._lifecycle

    @lifecycle.setter
    def lifecycle(self, value):
        self._lifecycle = value

    @property
    def tracking(self):
        return self._tracking

    @tracking.setter
    def tracking(self, value):
        self._tracking = value

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value

    @property
    def last_output(self):
        return self._last_output

    @last_output.setter
    def last_output(self, value):
        self._last_output = value

    @property
    def error_output(self):
        return self._error_output

    @error_output.setter
    def error_output(self, value):
        self._error_output = value

    @property
    def warnings(self):
        return self._warnings

    @warnings.setter
    def warnings(self, value):
        self._warnings = value

    def add_warning(self, warning):
        self._warnings.update([warning.name])

    @property
    def exec_error(self) -> ExecutionError:
        return self._exec_error

    @exec_error.setter
    def exec_error(self, value):
        self._exec_error = value

    @property
    def queue_waiter(self):
        return None

    def create_snapshot(self):
        return JobInst(
            self.metadata,
            self.lifecycle,
            self.tracking,
            self.status,
            self.error_output,
            self.warnings,
            self.exec_error)

    def stop(self):
        self.stopped = True

    def interrupted(self):
        self.interrupted = True

    def add_state_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY, notify_on_register=False):
        self.state_notification.add_observer(observer, priority)

    def remove_state_observer(self, observer):
        self.state_notification.remove_observer(observer)

    def add_warning_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        pass

    def remove_warning_observer(self, observer):
        pass

    def add_output_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        pass

    def remove_output_observer(self, observer):
        pass


@dataclass
class TestJobInstanceManager(JobInstanceManager):

    def __init__(self):
        self.instances = []

    def register_instance(self, job_instance):
        self.instances.append(job_instance)

    def unregister_instance(self, job_instance):
        self.instances.remove(job_instance)
