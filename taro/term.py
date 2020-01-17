import logging

log = logging.getLogger(__name__)


class Term:

    def __init__(self, job_control):
        self.job_control = job_control

    def terminate(self, _, __):
        log.warning('event=[terminated_by_signal]')
        self.job_control.interrupt()

    def interrupt(self, _, __):
        log.warning('event=[interrupted_by_keyboard]')
        self.job_control.interrupt()  # TODO handle repeated signal
