import os
import sys
from typing import Optional

from taro.jobs.job import InstanceMatchingCriteria


def instance_matching_criteria(args, def_id_match_strategy) -> Optional[InstanceMatchingCriteria]:
    if args.instances:
        return InstanceMatchingCriteria(args.instances, id_match_strategy=def_id_match_strategy)
    else:
        return None


def handle_broken_pipe(*, exit_code):
    # According to the official Python doc: https://docs.python.org/3/library/signal.html#note-on-sigpipe
    # Python flushes standard streams on exit; redirect remaining output
    # to devnull to avoid another BrokenPipeError at shutdown
    devnull = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull, sys.stdout.fileno())
    sys.exit(exit_code)  # Python exits with error code 1 on EPIPE
