from taro.jobs.execution import Flag,Phase
from taro.theme import Theme
from taro.util import DateTimeFormat


def job_id_style(job):
    if job.state.has_flag(Flag.FAILURE):
        return Theme.job + " " + Theme.state_failure
    return Theme.job

def job_id_stats_style(job_stats):
    if job_stats.last_state.has_flag(Flag.FAILURE):
        return Theme.job + " " + Theme.state_failure
    return Theme.job


def instance_style(job):
    if job.state.has_flag(Flag.FAILURE):
        return Theme.state_failure
    return Theme.instance


def general_style(job):
    if job.state.has_flag(Flag.FAILURE):
        return Theme.state_failure
    return ""

def stats_style(stats):
    if stats.last_state.has_flag(Flag.FAILURE):
        return Theme.state_failure
    return ""


def warn_style(_):
    return Theme.warning


def job_state_style(job):
    return state_style(job.state)

def stats_state_style(stats):
    return state_style(stats.last_state)


def state_style(state):
    if state.in_phase(Phase.SCHEDULED):
        return Theme.state_before_execution
    if state.in_phase(Phase.EXECUTING):
        return Theme.state_executing
    if state.has_flag(Flag.DISCARDED):
        return Theme.state_discarded
    if state.has_flag(Flag.FAILURE):
        return Theme.state_failure
    if state.has_flag(Flag.INCOMPLETE):
        return Theme.state_incomplete
    return ""

def stats_failed_style(stats):
    if stats.failed_count:
        return Theme.highlight + " " + Theme.state_failure
    return stats_style(stats)


def stats_warn_style(stats):
    if stats.warning_count:
        return Theme.warning
    return stats_style(stats)


def job_instance_styled(job_instance):
    return [
        (job_id_style(job_instance), job_instance.job_id),
        (Theme.id_separator, "@"),
        (instance_style(job_instance), job_instance.instance_id)
    ]


def job_instance_id_styled(job_instance_id):
    return [
        (Theme.job, job_instance_id.job_id),
        (Theme.id_separator, "@"),
        (Theme.instance, job_instance_id.instance_id)
    ]


def job_status_line_styled(job_instance, *, ts_prefix_format=DateTimeFormat.DATE_TIME_MS_LOCAL_ZONE):
    return job_instance_id_status_line_styled(
        job_instance.id, job_instance.state, job_instance.lifecycle.last_changed_at, ts_prefix_format=ts_prefix_format)


def job_instance_id_status_line_styled(
        job_instance_id, current_state, ts=None, *, ts_prefix_format=DateTimeFormat.DATE_TIME_MS_LOCAL_ZONE):
    style_text_tuples = \
        job_instance_id_styled(job_instance_id) + [("", " -> "), (state_style(current_state), current_state.name)]
    ts_prefix_formatted = ts_prefix_format(ts) if ts else None
    if ts_prefix_formatted:
        return [("", ts_prefix_formatted + " ")] + style_text_tuples
    else:
        return style_text_tuples
