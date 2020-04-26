# For plugin testing in test_plugin.py
from typing import List

from taro import PluginBase
from taro import JobInstance, JobControl


class ValidPlugin(PluginBase):
    INSTANCES: List['ValidPlugin'] = []

    def __init__(self):
        ValidPlugin.INSTANCES.append(self)
        self.job_instances: List[JobInstance] = []

    def new_job_instance(self, job_instance: JobControl):
        self.job_instances.append(job_instance)
