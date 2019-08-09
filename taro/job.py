from typing import Iterable, Union, Any

from taro.execution import Execution


def job_id(category: str, name: str):
    return "{}/{}".format(category, name)


class Job:
    def __init__(self, category: str, name: str, execution: Union[Execution, Any], notifications: Iterable[str] = ()):
        self.id = job_id(category, name)
        self.category = category
        self.name = name
        self.execution = execution
        self.notifications = list(notifications)

    def __repr__(self):
        return "{}({!r}, {!r}, {!r}, {!r})".format(
            self.__class__.__name__, self.category, self.name, self.execution, self.notifications)
