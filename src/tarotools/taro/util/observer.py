import sys
from _operator import itemgetter
from itertools import chain


DEFAULT_OBSERVER_PRIORITY = 100


class Notification:

    def __init__(self, logger=None, joined_notification=None):
        self._logger = logger
        self._joined_notification = joined_notification
        self._prioritized_observers = []

    @property
    def observers(self):
        return [o for _, o in self._prioritized_observers]

    @property
    def prioritized_observers(self):
        return list(self._prioritized_observers)

    def _notify(self, observer, *args) -> bool:
        return False

    def add_observer(self, observer, priority=DEFAULT_OBSERVER_PRIORITY):
        self._prioritized_observers = sorted(
            chain(self._prioritized_observers, [(priority, observer)]),
            key=itemgetter(0))

    def remove_observer(self, observer):
        self._prioritized_observers = [(priority, o) for priority, o in self._prioritized_observers if o != observer]

    def notify_all(self, *args):
        if self._joined_notification:
            all_observers = sorted(chain(
                self._prioritized_observers, self._joined_notification._prioritized_observers), key=itemgetter(0))
        else:
            all_observers = self._prioritized_observers

        for _, observer in all_observers:
            # noinspection PyBroadException
            try:
                if not self._notify(observer, *args):
                    if callable(observer):
                        observer(*args)
                    else:
                        if self._logger:
                            self._logger.warning("event=[unsupported_observer] observer=[%s]", observer)
            except Exception as e:
                if self._logger:
                    self._logger.exception("event=[observer_exception]")
                else:
                    print(e, file=sys.stderr)
