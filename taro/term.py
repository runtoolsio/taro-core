import logging

log = logging.getLogger(__name__)


class Term:

    def __init__(self, execution):
        self.execution = execution

    def terminate(self, _, __):
        log.warning('event=[terminated_by_signal]')
        self.execution.interrupt(15)

    def interrupt(self, _, __):
        log.warning('event=[interrupted_by_keyboard]')
        self.execution.interrupt(2)  # TODO handle repeated signal
