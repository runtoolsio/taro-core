from taro import JobInfo


def i(metadata=None, lifecycle=None, tracking=None, status=None, error_output=None, warnings=None, exec_error=None):
    return JobInfo(metadata, lifecycle, tracking, status, error_output, warnings, exec_error)
