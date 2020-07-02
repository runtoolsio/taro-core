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

    jobs = args.jobs
    disabled_jobs = [DisabledJob(j, args.regex, utc_now(), None) for j in args.jobs]
    try:
        persistence.add_disabled_jobs(disabled_jobs)
        print("Jobs disabled: {}".format(",".join(jobs)))
    finally:
        persistence.close()
