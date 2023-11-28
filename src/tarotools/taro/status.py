from __future__ import annotations

DEFAULT_OBSERVER_PRIORITY = 100


class TaskBuilder:

    def event(self, event):
        pass

    def task(self, task_name) -> TaskBuilder:
        pass

