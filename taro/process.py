import subprocess

from taro.execution import Execution, ExecutionState


class ProcessExecution(Execution):

    def __init__(self, args):
        self.args = args

    def execute(self) -> ExecutionState:
        subprocess.call(self.args)
        return ExecutionState.COMPLETED
