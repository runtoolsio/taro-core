import re


class AllFilter:

    def __init__(self):
        self.filters = []

    def __ilshift__(self, j_filter):
        self.filters.append(j_filter)
        return self

    def __call__(self, job_instance):
        return all(f(job_instance) for f in self.filters)


def create_id_filter(text):
    pattern = re.compile(text)

    def do_filter(job_instance):
        return pattern.search(job_instance.job_id) or pattern.search(job_instance.instance_id)

    return do_filter


def finished_filter(job_instance):
    return job_instance.lifecycle.state().is_terminal()
