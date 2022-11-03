from taro.theme import Theme
from taroapp import printer


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


def job_instance_styled(job_instance):
    return [
        (job_style(job_instance), job_instance.job_id),
        ("", "@"),
        (instance_style(job_instance), job_instance.instance_id)
    ]


def job_status_line_styled(job_instance, *, prefix_ts=True):
    style_text_tuples =\
        job_instance_styled(job_instance) + [("", " -> "), (state_style(job_instance), job_instance.state.name)]
    if prefix_ts:
        return [("", printer.format_dt(job_instance.lifecycle.last_changed()) + " ")] + style_text_tuples
    else:
        return style_text_tuples


def job_instance_id_styled(job_id, instance_id):
    return [(Theme.job, job_id), ("", "@"), (Theme.instance, instance_id)]
