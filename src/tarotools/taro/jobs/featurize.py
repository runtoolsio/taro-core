import logging
from dataclasses import dataclass
from threading import Lock
from typing import Optional, Tuple, Callable, TypeVar, Generic, Dict

from tarotools.taro import InstanceStateObserver, JobInst, JobInstance, JobInstanceID
from tarotools.taro import persistence as persistence_mod
from tarotools.taro.err import InvalidStateError
from tarotools.taro.jobs.api import APIServer
from tarotools.taro.jobs.events import StateDispatcher, OutputDispatcher
from tarotools.taro.jobs.execution import ExecutionPhase
from tarotools.taro.jobs.inst import JobInstanceManager, InstanceOutputObserver

log = logging.getLogger(__name__)

F = TypeVar('F')


@dataclass
class Feature(Generic[F]):
    feature: F
    open_hook: Optional[Callable[[], None]] = None
    close_hook: Optional[Callable[[], None]] = None


@dataclass
class ManagerFeature(Feature):
    unregister_terminated_instances: bool = False


@dataclass
class ObserverFeature(Feature):
    priority: int = 100


def _convert_hook(feature, hook):
    def wrapped_hook():
        return hook(feature)

    return wrapped_hook if hook else None


def _create_observer_features(factories):
    observers = []
    for factory, open_hook, close_hook, priority in factories:
        feature = factory()
        cnv_open_hook = _convert_hook(feature, open_hook)
        cnv_close_hook = _convert_hook(feature, close_hook)

        observers.append(ObserverFeature(feature, cnv_open_hook, cnv_close_hook, priority))

    return observers


class FeaturedContextBuilder:

    def __init__(self, *, keep_removed=False):
        self._instance_managers = []
        self._state_observers = []
        self._output_observers = []
        self._keep_removed = keep_removed

    def __call__(self):
        self.build()

    def add_instance_manager(self, factory, open_hook=None, close_hook=None, unregister_terminated_instances=False) \
            -> 'FeaturedContextBuilder':
        self._instance_managers.append((factory, open_hook, close_hook, unregister_terminated_instances))
        return self

    def add_state_observer(self, factory, open_hook=None, close_hook=None, priority=100) -> 'FeaturedContextBuilder':
        self._state_observers.append((factory, open_hook, close_hook, priority))
        return self

    def add_output_observer(self, factory, open_hook=None, close_hook=None, priority=100) -> 'FeaturedContextBuilder':
        self._output_observers.append((factory, open_hook, close_hook, priority))
        return self

    def standard_features(
            self, api_server=True, state_dispatcher=True, output_dispatcher=True, persistence=True, plugins=True):
        if api_server:
            self.add_instance_manager(APIServer, lambda api: api.open(), lambda api: api.close())
        if state_dispatcher:
            self.add_state_observer(StateDispatcher, close_hook=lambda dispatcher: dispatcher.close())
        if output_dispatcher:
            self.add_state_observer(OutputDispatcher, close_hook=lambda dispatcher: dispatcher.close())
        if persistence:
            # Lower default priority so other listeners can see the instance already persisted
            self.add_state_observer(
                persistence_mod.load_configured_persistence, close_hook=lambda db: db.close(), priority=50)
        # TODO plugins

    def build(self) -> 'FeaturedContext':
        instance_managers = []
        for factory, open_hook, close_hook, unregister_terminated in self._instance_managers:
            feature = factory()
            cnv_open_hook = _convert_hook(feature, open_hook)
            cnv_close_hook = _convert_hook(feature, close_hook)

            instance_managers.append(ManagerFeature(feature, cnv_open_hook, cnv_close_hook, unregister_terminated))

        state_observers = _create_observer_features(self._state_observers)
        output_observers = _create_observer_features(self._output_observers)

        return FeaturedContext(instance_managers, state_observers, output_observers, keep_removed=self._keep_removed)


@dataclass
class _ManagedInstance:
    instance: JobInstance
    releasing: bool = False  # Must be guarded by the lock
    released: bool = False


class FeaturedContext(InstanceStateObserver):

    def __init__(self, instance_managers=(), state_observers=(), output_observers=(), *, keep_removed=False):
        self._instance_managers: Tuple[ManagerFeature[JobInstanceManager]] = tuple(instance_managers)
        self._state_observers: Tuple[ObserverFeature[InstanceStateObserver]] = tuple(state_observers)
        self._output_observers: Tuple[ObserverFeature[InstanceOutputObserver]] = tuple(output_observers)
        self._keep_removed = keep_removed
        self._managed_instances: Dict[JobInstanceID, _ManagedInstance] = {}
        self._ctx_lock = Lock()
        self._opened = False
        self._close_requested = False
        self._closed = False

    @property
    def instances(self):
        with self._ctx_lock:
            return list(managed.instance for managed in self._managed_instances.values())

    def get_instance(self, job_instance_id) -> Optional[JobInstance]:
        with self._ctx_lock:
            managed = self._managed_instances.get(job_instance_id)

        return managed.instance if managed else None

    def __enter__(self):
        if self._opened:
            raise InvalidStateError("Managed job context has been already opened")

        self._execute_open_hooks()
        self._opened = True

        return self

    def _execute_open_hooks(self):
        for open_hook in (f.open_hook for f in
                          (self._instance_managers + self._state_observers + self._output_observers) if f.open_hook):
            open_hook()

    def add(self, job_instance):
        with self._ctx_lock:
            if not self._opened:
                raise InvalidStateError("Cannot add job instance because the context has not been opened")
            if self._close_requested:
                raise InvalidStateError("Cannot add job instance because the context has been already closed")
            if job_instance.id in self._managed_instances:
                raise ValueError("An instance with this ID has already been added to the context")

            managed_instance = _ManagedInstance(job_instance)
            self._managed_instances[job_instance.id] = managed_instance

        for manager_feat in self._instance_managers:
            manager_feat.feature.register_instance(job_instance)

        ctx_observer_priority = 1000
        for state_observer_feat in self._state_observers:
            job_instance.add_state_observer(state_observer_feat.feature, state_observer_feat.priority)
            ctx_observer_priority = max(ctx_observer_priority, state_observer_feat.priority)

        for output_observer_feat in self._output_observers:
            job_instance.add_output_observer(output_observer_feat.feature, output_observer_feat.priority)

        # #  TODO optional plugins
        # if cfg.plugins_enabled and cfg.plugins_load:
        #     plugins.register_new_job_instance(job_instance, cfg.plugins_load,
        #                                       plugin_module_prefix=EXT_PLUGIN_MODULE_PREFIX)

        # Add observer first and only then check for termination to prevent release miss by the race condition
        job_instance.add_state_observer(self, ctx_observer_priority + 1)
        if job_instance.lifecycle.ended:
            self._release_instance(job_instance.id, not self._keep_removed)

        return job_instance

    def remove(self, job_instance_id) -> Optional[JobInstance]:
        return self._release_instance(job_instance_id, True)

    def new_instance_state(self, job_inst: JobInst, previous_state, new_state, changed):
        if new_state.in_phase(ExecutionPhase.TERMINAL):
            self._release_instance(job_inst.id, not self._keep_removed)

    def _release_instance(self, job_instance_id, remove):
        """
        Implementation note: A race condition can cause this method to be executed twice with the same ID
        """
        release = False
        removed = False
        with self._ctx_lock:
            managed_instance = self._managed_instances.get(job_instance_id)
            if not managed_instance:
                return None  # The instance has been removed before termination

            if not managed_instance.releasing:
                managed_instance.releasing = release = True  # The flag is guarded by the lock

            if remove:
                del self._managed_instances[job_instance_id]
                removed = True

        job_instance = managed_instance.instance
        if release:
            job_instance.remove_state_observer(self)

            for output_observer_feat in self._output_observers:
                job_instance.remove_output_observer(output_observer_feat.feature)

            for state_observer_feat in self._state_observers:
                job_instance.remove_state_observer(state_observer_feat.feature)

            for manager_feat in self._instance_managers:
                if manager_feat.unregister_terminated_instances:
                    manager_feat.feature.unregister_instance(job_instance)

            managed_instance.released = True

        if removed:
            for manager_feat in self._instance_managers:
                if not manager_feat.unregister_terminated_instances:
                    manager_feat.feature.unregister_instance(job_instance)

        self._check_close()
        return job_instance

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._close_requested:
            return

        self._close_requested = True
        self._check_close()

    def _check_close(self):
        close = False
        with self._ctx_lock:
            if not self._close_requested or self._closed:
                return

            all_released = all(managed.released for managed in self._managed_instances.values())
            if all_released:
                self._closed = close = True

        if close:
            self._execute_close_hooks()

    def _execute_close_hooks(self):
        for close_hook in (f.close_hook for f in
                           (self._instance_managers + self._state_observers + self._output_observers) if f.close_hook):
            # noinspection PyBroadException
            try:
                close_hook()
            except BaseException:
                log.exception("event=[close_hook_error] hook=[%s]", close_hook)
