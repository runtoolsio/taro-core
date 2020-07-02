from taro import cnf, persistence, ps
from taro.view import disabled as view_dis


def run(args):
    cnf.init(args)
    persistence_enabled = persistence.init()
    if not persistence_enabled:
        print("Persistence is disabled")
        exit(1)

    try:
        disabled_jobs = persistence.read_disabled_jobs()
        ps.print_table(disabled_jobs, view_dis.DEFAULT_COLUMNS, show_header=True, pager=False)
    finally:
        persistence.close()
