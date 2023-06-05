from taro.jobs import persistence
from taro.util import MatchingStrategy
from taroapp import argsutil, printer
from taroapp.view import stats


def run(args):
    instance_match = argsutil.instance_matching_criteria(args, MatchingStrategy.PARTIAL)
    job_stats_list = persistence.read_stats(instance_match)
    printer.print_table(job_stats_list, stats.DEFAULT_COLUMNS, show_header=True, pager=True)
