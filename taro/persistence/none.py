class NoPersistence:

    def read_jobs(self, *, sort, asc, limit):
        raise PersistenceDisabledError()

    def store_job(self, job_info):
        raise PersistenceDisabledError()

    def add_disabled_jobs(self, disabled_jobs):
        raise PersistenceDisabledError()

    def remove_disabled_jobs(self, job_ids):
        raise PersistenceDisabledError()

    def read_disabled_jobs(self):
        raise PersistenceDisabledError()

    def close(self):
        pass


class PersistenceDisabledError(Exception):

    def __init__(self):
        super().__init__('Executed logic depending on data persistence. However persistence is disabled in the config.')
