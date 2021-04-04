from taro import client
from taroapp import ps
from taroapp.view import instance as view_inst


def run(args):
    jobs = client.read_jobs_info(args.inst)
    ps.print_table(jobs, view_inst.DEFAULT_COLUMNS, _colours, show_header=True, pager=False)


def _colours(job_info):
    state = job_info.state

    if state.is_before_execution():
        return '#44aaff'

    if state.is_executing():
        return 'green'

    if state.is_failure():
        return 'red'

    if state.is_unexecuted() or job_info.warnings:
        return 'orange'

    return ''
