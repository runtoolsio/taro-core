# For plugin testing in test_plugin.py and test_app_exec_plugin.py
from typing import List, Optional
from weakref import ref

from tarotools.taro import JobInstance
from tarotools.taro import Plugin


class TestPlugin(Plugin):

    instance_ref: Optional["ref"] = None
    error_on_new_job_instance: Optional[BaseException] = None

    def __init__(self):
        TestPlugin.instance_ref = ref(self)
        self.job_instances: List[JobInstance] = []

    def register_instance(self, job_instance):
        self.job_instances.append(job_instance)
        error_to_raise = TestPlugin.error_on_new_job_instance
        TestPlugin.error_on_new_job_instance = None
        if error_to_raise:
            raise error_to_raise

    def unregister_instance(self, job_instance):
        pass

    def unregister_after_termination(self):
        return False

    def close(self):
        pass
