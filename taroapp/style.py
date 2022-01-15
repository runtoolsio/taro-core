from prompt_toolkit.formatted_text import FormattedText

from taro.theme import Theme


def job_style(job):
    if job.state.is_failure():
        return Theme.job + " " + Theme.state_failure
    return Theme.job


def instance_style(job):
    if job.state.is_failure():
        return Theme.state_failure
    return Theme.instance


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


def job_instance(job):
    return FormattedText([(job_style(job), job.job_id), ("", "@"), (instance_style(job), job.instance_id)])


def job_status_line(job):
    return FormattedText(job_instance(job) + FormattedText([("", " -> "), ((state_style(job)), job.state.name)]))
