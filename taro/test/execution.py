import logging
from taro.execution import Execution, ExecutionState

log = logging.getLogger(__name__)


class TestExecution(Execution):

    def __init__(self, after_exec_state: ExecutionState = None, raise_exc: Exception = None):
        if after_exec_state and raise_exc:
            raise ValueError("both after_exec_state and throw_exc are set", after_exec_state, raise_exc)
        self.after_exec_state = after_exec_state or ExecutionState.COMPLETED
        self.raise_exc = raise_exc

    def __repr__(self):
        return "{}({!r})".format(self.__class__.__name__, self.after_exec_state.name)

    def execute(self):
        if self.after_exec_state:
            log.info('event=[executed] new_state=[{}]', self.after_exec_state)
            return self.after_exec_state
        else:
            log.info('event=[executed] exception_raised=[{}]', self.raise_exc)
            raise self.raise_exc
