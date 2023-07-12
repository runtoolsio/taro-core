from tarotools.taro import JobInst, JobInstanceID
from tarotools.taro import util
from tarotools.taro.jobs.inst import JobInstanceMetadata


def i(job_id, instance_id=None, params=None, user_params=None, lifecycle=None, tracking=None, status=None, error_output=None, warnings=None, exec_error=None):
    meta = JobInstanceMetadata(JobInstanceID(job_id, instance_id or util.unique_timestamp_hex()), params, user_params)
    return JobInst(meta, lifecycle, tracking, status, error_output, warnings, exec_error)
