from taro.jobs import persistence


def run(args):
    removed = persistence.remove_disabled_jobs(args.jobs)
    if removed:
        print("Removed from disabled jobs: " + ", ".join(removed))
    else:
        print("None of provided jobs is disabled")
