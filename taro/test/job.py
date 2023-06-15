from taro import JobInfo, util, JobInstanceID
from taro.jobs.inst import JobInstanceMetadata


def i(job_id, instance_id=None, user_params=None, params=None, lifecycle=None, tracking=None, status=None, error_output=None, warnings=None, exec_error=None):
    meta = JobInstanceMetadata(JobInstanceID(job_id, instance_id or util.unique_timestamp_hex()), user_params, params)
    return JobInfo(meta, lifecycle, tracking, status, error_output, warnings, exec_error)
