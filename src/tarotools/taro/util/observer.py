import sys
from _operator import itemgetter
from itertools import chain
from typing import List, Tuple, Any, Callable, Optional, TypeVar, Generic

DEFAULT_OBSERVER_PRIORITY = 100


class CallableNotification:

    def __init__(self, *, error_hook: Optional[Callable[[Callable, Tuple[Any], Exception], None]] = None):
        self.error_hook: Optional[Callable[[Callable, Tuple[Any], Exception], None]] = error_hook
        self._prioritized_observers = []

    def __call__(self, *args):
        self.notify_all(*args)

    @property
    def observers(self) -> List[Callable[..., Any]]:
        return [o for _, o in self._prioritized_observers]

    @property
    def prioritized_observers(self) -> List[Tuple[int, Callable[..., Any]]]:
        return list(self._prioritized_observers)

    def add_observer(self, observer: Callable[..., Any], priority: int = DEFAULT_OBSERVER_PRIORITY) -> None:
        self._prioritized_observers = sorted(
            chain(self._prioritized_observers, [(priority, observer)]),
            key=itemgetter(0))

    def remove_observer(self, observer: Callable[..., Any]) -> None:
        self._prioritized_observers = [(priority, o) for priority, o in self._prioritized_observers if o != observer]

    def notify_all(self, *args):
        for _, observer in self._prioritized_observers:
            # noinspection PyBroadException
            try:
                observer(*args)
            except Exception as e:
                if self.error_hook:
                    self.error_hook(observer, args, e)
                else:
                    print(f"{e.__class__.__name__}: {e}", file=sys.stderr)


O = TypeVar("O")


class ObservableNotification(Generic[O]):

    def __init__(self, error_hook: Optional[Callable[[O, Tuple[Any], Exception], None]] = None):
        self.error_hook: Optional[Callable[[O, Tuple[Any], Exception], None]] = error_hook
        self._prioritized_observers = []
        self._observer_proxy = _Proxy(self)

    @property
    def observer_proxy(self) -> O:
        return self._observer_proxy

    @property
    def observers(self) -> List[O]:
        return [o for _, o in self._prioritized_observers]

    @property
    def prioritized_observers(self) -> List[Tuple[int, O]]:
        return list(self._prioritized_observers)

    def add_observer(self, observer: O, priority: int = DEFAULT_OBSERVER_PRIORITY) -> None:
        self._prioritized_observers = sorted(
            chain(self._prioritized_observers, [(priority, observer)]),
            key=itemgetter(0))

    def remove_observer(self, observer: O) -> None:
        self._prioritized_observers = [(priority, o) for priority, o in self._prioritized_observers if o != observer]


class _Proxy(Generic[O]):

    def __init__(self, notification: ObservableNotification) -> None:
        self._notification = notification

    def __getattribute__(self, name: str) -> object:
        def method(*args, **kwargs):
            for observer in object.__getattribute__(self, "_notification").observers:
                try:
                    getattr(observer, name)(*args, **kwargs)
                except Exception as e:
                    error_hook = object.__getattribute__(self, "_notification").error_hook
                    if error_hook:
                        error_hook(observer, args, e)
                    else:
                        print(f"{e.__class__.__name__}: {e}", file=sys.stderr)

        # Special handling for methods/attributes that are specific to the proxy object itself
        if name in ["_notification"]:
            return object.__getattribute__(self, name)

        return method
