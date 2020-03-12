from taro.job import ExecutionStateObserver


class Persistence(ExecutionStateObserver):

    def __init__(self, connection):
        self._connection = connection

    def notify(self, job_instance):
        pass