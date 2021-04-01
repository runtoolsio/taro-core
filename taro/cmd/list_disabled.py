from taro import persistence, ps
from taro.view import disabled as view_dis


def run(args):
    disabled_jobs = persistence.read_disabled_jobs()
    ps.print_table(disabled_jobs, view_dis.DEFAULT_COLUMNS, show_header=True, pager=False)
