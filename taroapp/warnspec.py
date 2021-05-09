import re

from taro import warning

EXEC_TIME_WARN_REGEX = r'exec_time>(\d+)([smh])'
OUTPUT_MATCHES_REGEX = 'output=~(.+)'
FILE_CONTAINS_REGEX = 'file:(.+)=~(.+)'


def setup_warnings(job_instance, *warn_specs: str):
    for warn_spec in warn_specs:
        w_spec_no_space = warn_spec.replace(" ", "").rstrip()
        _init_warning(job_instance, w_spec_no_space)


def _init_warning(job_instance, warn_spec):
    m = re.compile(EXEC_TIME_WARN_REGEX).match(warn_spec)
    if m:
        _exec_time_exceeded(job_instance, m)
        return

    m = re.compile(OUTPUT_MATCHES_REGEX).match(warn_spec)
    if m:
        _output_matches(job_instance, m)
        return

    raise ValueError("Invalid warning specification: " + warn_spec)

    # TODO
    # m = re.compile(FILE_CONTAINS_REGEX).match(warn_spec)
    # if m:
    #     file = m.group(1)
    #     regex = m.group(2)
    #     return FileLineMatchesWarning(m.group(0), file, regex)


def _exec_time_exceeded(job_instance, w_spec_match):
    value = int(w_spec_match.group(1))
    unit = w_spec_match.group(2)
    if unit == 'm':
        value *= 60
    if unit == 'h':
        value *= 60 * 60

    warning.exec_time_exceeded(job_instance, w_spec_match.group(0), value)


def _output_matches(job_instance, w_spec_match):
    warning.output_matches(job_instance, w_spec_match.group(0), w_spec_match.group(1))
