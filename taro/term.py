import logging

log = logging.getLogger(__name__)


class Term:

    def __init__(self, job_instance):
        self.job_instance = job_instance

    def terminate(self, _, __):
        log.warning('event=[terminated_by_signal]')
        self.job_instance.interrupt()

    def interrupt(self, _, __):
        log.warning('event=[interrupted_by_keyboard]')
        self.job_instance.interrupt()  # TODO handle repeated signal
