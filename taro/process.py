from taro import ExecutionState
from taro.execution import OutputExecution


class ProcessExecution(OutputExecution):

    def __init__(self, target, args):
        self.target = target
        self.args = args

    def is_async(self) -> bool:
        return False

    def execute(self) -> ExecutionState:
        pass

    def add_output_observer(self, observer):
        pass

    def remove_output_observer(self, observer):
        pass

    def status(self):
        pass

    def stop(self):
        pass

    def interrupt(self):
        pass