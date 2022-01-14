from taro.theme import Theme


def general_style(job):
    if job.state.is_failure():
        return Theme.state_failure
    return ""


def warn_style(_):
    return Theme.warning


def state_style(job):
    if job.state.is_before_execution():
        return Theme.state_before_execution
    if job.state.is_executing():
        return Theme.state_executing
    if job.state.is_incomplete():
        return Theme.state_incomplete
    if job.state.is_unexecuted():
        return Theme.state_not_executed
    if job.state.is_failure():
        return Theme.state_failure
    return ""
