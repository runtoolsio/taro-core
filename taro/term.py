import logging

log = logging.getLogger(__name__)


class Term:

    def __init__(self, execution):
        self.execution = execution

    def terminate(self, _, __):
        log.warning('event=[terminated_by_signal]')
        self.execution.stop_execution()
