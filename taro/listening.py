#  Sender, Listening
from taro.job import ExecutionStateObserver

LISTENER_FILE_EXTENSION = '.listener'


class Dispatcher(ExecutionStateObserver):

    def notify(self, job_instance):
        pass
