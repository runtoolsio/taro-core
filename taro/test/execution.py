import logging
from taro.execution import Execution, ExecutionState

log = logging.getLogger(__name__)


class TestExecution(Execution):

    def __init__(self, after_exec_state: str = ExecutionState.COMPLETED.name):
        self.after_exec_state = ExecutionState[after_exec_state.upper()]

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self.after_exec_state.name)

    def execute(self):
        log.info('event=[executed] new_state=[{}]', self.after_exec_state)
        return self.after_exec_state
