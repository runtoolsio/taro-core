import sys

from taro import cnf, persistence
from taro.job import DisabledJob
from taro.util import utc_now


def run(args):
    cnf.init(args)
    persistence_enabled = persistence.init()

    if not persistence_enabled:
        print('Persistence is disabled. Enable persistence in config file to be able to store disabled jobs',
              file=sys.stderr)
        exit(1)

    disabled_jobs = [DisabledJob(j, args.regex, utc_now(), None) for j in args.jobs]
    try:
        added = persistence.add_disabled_jobs(disabled_jobs)
        if added:
            print("Added to disabled jobs: " + ",".join([a.job_id for a in added]))
        else:
            print("Already disabled")

    finally:
        persistence.close()
