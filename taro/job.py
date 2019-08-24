from typing import Iterable, Union, Any

from taro.execution import Execution


class Job:
    def __init__(self, job_id: str, execution: Union[Execution, Any], observers: Iterable[str] = ()):
        self.id = job_id
        self.execution = execution
        self.observers = list(observers)

    def __repr__(self):
        return "{}({!r}, {!r}, {!r})".format(
            self.__class__.__name__, self.id, self.execution, self.observers)
