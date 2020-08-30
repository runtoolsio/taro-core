from taro import ps, client
from taro.view import instance as view_inst


def run(args):
    jobs = client.read_jobs_info(args.inst)
    ps.print_table(jobs, view_inst.DEFAULT_COLUMNS, show_header=True, pager=False)
