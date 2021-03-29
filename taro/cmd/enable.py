import sys

from taro import persistence


def run(args):
    persistence_enabled = persistence.init()

    if not persistence_enabled:
        print('Persistence is disabled. Enable persistence in config file to be able to remove disabled jobs',
              file=sys.stderr)
        exit(1)

    try:
        removed = persistence.remove_disabled_jobs(args.jobs)
        if removed:
            print("Removed from disabled jobs: " + ", ".join(removed))
        else:
            print("None of provided jobs is disabled")

    finally:
        persistence.close()
