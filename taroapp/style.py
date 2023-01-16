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


def job_state_style(job):
    return state_style(job.state)


def state_style(state):
    if state.is_before_execution():
        return Theme.state_before_execution
    if state.is_executing():
        return Theme.state_executing
    if state.is_incomplete():
        return Theme.state_incomplete
    if state.is_unexecuted():
        return Theme.state_not_executed
    if state.is_failure():
        return Theme.state_failure
    return ""


def job_instance_styled(job_instance):
    return [
        (job_style(job_instance), job_instance.job_id),
        (Theme.id_separator, "@"),
        (instance_style(job_instance), job_instance.instance_id)
    ]


def job_instance_id_styled(job_instance_id):
    return [
        (Theme.job, job_instance_id.job_id),
        (Theme.id_separator, "@"),
        (Theme.instance, job_instance_id.instance_id)
    ]


def job_status_line_styled(job_instance, *, prefix_ts=True):
    changed = job_instance.lifecycle.last_changed if prefix_ts else None
    return job_instance_id_status_line_styled(job_instance.id, job_instance.state, changed)


def job_instance_id_status_line_styled(job_instance_id, current_state, ts=None):
    style_text_tuples = \
        job_instance_id_styled(job_instance_id) + [("", " -> "), (state_style(current_state), current_state.name)]
    if ts:
        return [("", printer.format_dt(ts) + " ")] + style_text_tuples
    else:
        return style_text_tuples
