from taro import ps
from taro.api import Client
from taro.view import instance as view_inst


def run(args):
    with Client() as client:
        jobs = client.read_jobs_info()
    ps.print_table(jobs, view_inst.DEFAULT_COLUMNS, show_header=True, pager=False)
