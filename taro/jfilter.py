import re


class AllFilter:

    def __init__(self):
        self.filters = []

    def __lshift__(self, j_filter):
        self.filters.append(j_filter)

    def __call__(self, job_instance):
        return all(f(job_instance) for f in self.filters)


def create_id_filter(text):
    pattern = re.compile(text)

    def do_filter(job):
        return pattern.search(job.job_id) or pattern.search(job.instance_id)

    return do_filter
