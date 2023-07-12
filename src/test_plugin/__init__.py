# For plugin testing in test_plugin.py and test_app_exec_plugin.py
from typing import List, Optional
from weakref import ref

from tarotools.taro import JobInstance
from tarotools.taro import PluginBase


class TestPlugin(PluginBase):
    instance_ref: Optional["ref"] = None
    error_on_new_job_instance: Optional[BaseException] = None

    def __init__(self):
        TestPlugin.instance_ref = ref(self)
        self.job_instances: List[JobInstance] = []

    def new_job_instance(self, job_instance: JobInstance):
        self.job_instances.append(job_instance)
        error_to_raise = TestPlugin.error_on_new_job_instance
        TestPlugin.error_on_new_job_instance = None
        if error_to_raise:
            raise error_to_raise
