from taro.jobs import persistence
from taro.jobs.job import DisabledJob
from taro.util import utc_now


def run(args):
    disabled_jobs = [DisabledJob(j, args.regex, utc_now(), None) for j in args.jobs]
    added = persistence.add_disabled_jobs(disabled_jobs)
    if added:
        print("Added to disabled jobs: " + ", ".join([a.job_id for a in added]))
    else:
        print("Already disabled")
