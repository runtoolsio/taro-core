"""
This module provides tools for adding custom and built-in features to job instances using a specialized context,
referred to as a 'featured context'. When job instances are added to this context, they automatically gain the
features defined by the context.

The context serves dual roles:
1. As a container that holds job instances.
2. As an enabler of features for these instances.

If the context is configured as 'transient', job instances are auto-removed upon reaching their termination state.
"""

import logging
from dataclasses import dataclass
from threading import Lock
from typing import Optional, Tuple, Callable, TypeVar, Generic, Dict

from tarotools.taro import persistence as persistence_mod
from tarotools.taro import plugins as plugins_mod
from tarotools.taro.err import InvalidStateError
from tarotools.taro.jobs.api import APIServer
from tarotools.taro.jobs.events import InstanceTransitionDispatcher, OutputDispatcher
from tarotools.taro.jobs.instance import (InstanceTransitionObserver, JobRun, JobInstance, JobInstanceManager,
                                          InstanceOutputObserver)
from tarotools.taro.jobs.plugins import Plugin
from tarotools.taro.run import RunState

log = logging.getLogger(__name__)

C = TypeVar('C')


@dataclass
class Feature(Generic[C]):
    """Represents a feature to be added to the context.

    Attributes:
        component (Generic[C]): The main component representing the feature.
        open_hook (Optional[Callable[[], None]]): A hook called when the context is opened. Default is None.
        close_hook (Optional[Callable[[], None]]): A hook called after the context has been closed. Default is None.
    """
    component: C
    open_hook: Optional[Callable[[], None]] = None
    close_hook: Optional[Callable[[], None]] = None


@dataclass
class ManagerFeature(Feature[JobInstanceManager]):
    """
    The attribute `unregister_after_termination` can be set to `True` if the manager should
    not retain instances immediately after they have ended.

    Attributes:
        unregister_after_termination (bool):
            If set to `True`, every instance will be unregistered as soon as it reaches its terminal phase,
            regardless of the context's `transient` configuration. Default is False.
    """
    unregister_after_termination: bool = False


@dataclass
class ObserverFeature(Feature):
    """
    Attributes:
        priority (int):
            The priority value assigned when this observer is registered to a job instance.
            A lower value indicates higher priority. Default is 100.
    """
    priority: int = 100


def _convert_hook(component, hook):
    def wrapped_hook():
        return hook(component)

    return wrapped_hook if hook else None


def _create_observer_features(factories):
    observers = []
    for factory, open_hook, close_hook, priority in factories:
        component = factory()
        cnv_open_hook = _convert_hook(component, open_hook)
        cnv_close_hook = _convert_hook(component, close_hook)

        observers.append(ObserverFeature(component, cnv_open_hook, cnv_close_hook, priority))

    return observers


def _create_plugins(names):
    plugins_mod.load_modules(names)
    fetched_plugins = Plugin.fetch_plugins(names)

    plugin_features = []
    for plugin in fetched_plugins.values():
        feature = ManagerFeature(plugin, None, plugin.close, plugin.unregister_after_termination())
        plugin_features.append(feature)

    return plugin_features


class FeaturedContextBuilder:
    """
    This class offers a convenient and clear way to define a featured context.
    The simplest approach is to create the context using the `standard_features` method.
    Additionally, the builder can also serve as a provider, allowing it to be reused
    through repeated execution of the `build` method or, if more suitable, via the `__call__` mechanism:
    ```
    ctxProvider = FeaturedContextBuilder()...
    newCtx = ctxProvider()
    ```
    The builder re-instantiates the features every time a new context is constructed.
    """

    def __init__(self, *, transient=False):
        """
        Args:
            transient (bool): Transient context auto-removes terminated instances
        """
        self._instance_managers = []
        self._state_observers = []
        self._output_observers = []
        self._plugins = []
        self._transient = transient

    def __call__(self):
        self.build()

    def add_instance_manager(self, factory, open_hook=None, close_hook=None, unregister_after_termination=False) \
            -> 'FeaturedContextBuilder':
        self._instance_managers.append((factory, open_hook, close_hook, unregister_after_termination))
        return self

    def add_phase_transition_callback(self, factory, open_hook=None, close_hook=None, priority=100) -> 'FeaturedContextBuilder':
        self._state_observers.append((factory, open_hook, close_hook, priority))
        return self

    def add_output_observer(self, factory, open_hook=None, close_hook=None, priority=100) -> 'FeaturedContextBuilder':
        self._output_observers.append((factory, open_hook, close_hook, priority))
        return self

    def standard_features(
            self, api_server=True, state_dispatcher=True, output_dispatcher=True, persistence=True, plugins=()):
        if api_server:
            self.api_server()
        if state_dispatcher:
            self.phase_transition_dispatcher()
        if output_dispatcher:
            self.output_dispatcher()
        if persistence:
            self.persistence()
        if plugins:
            self.plugins(plugins)

        return self

    def api_server(self):
        self.add_instance_manager(APIServer, lambda api: api.start(), lambda api: api.close())
        return self

    def phase_transition_dispatcher(self):
        self.add_phase_transition_callback(InstanceTransitionDispatcher, close_hook=lambda dispatcher: dispatcher.close())
        return self

    def output_dispatcher(self):
        self.add_output_observer(OutputDispatcher, close_hook=lambda dispatcher: dispatcher.close())
        return self

    def persistence(self):
        # Lower default priority so other listeners can see the instance already persisted
        self.add_phase_transition_callback(
            persistence_mod.load_configured_persistence, close_hook=lambda db: db.close(), priority=50)
        return self

    def plugins(self, names):
        self._plugins = names
        return self

    def build(self) -> 'FeaturedContext':
        instance_managers = []
        for factory, open_hook, close_hook, unregister_terminated in self._instance_managers:
            component = factory()
            cnv_open_hook = _convert_hook(component, open_hook)
            cnv_close_hook = _convert_hook(component, close_hook)

            instance_managers.append(ManagerFeature(component, cnv_open_hook, cnv_close_hook, unregister_terminated))

        state_observers = _create_observer_features(self._state_observers)
        output_observers = _create_observer_features(self._output_observers)

        if self._plugins:
            instance_managers += _create_plugins(self._plugins)

        return FeaturedContext(instance_managers, state_observers, output_observers, transient=self._transient)


@dataclass
class _ManagedInstance:
    instance: JobInstance
    releasing: bool = False  # Must be guarded by the lock
    released: bool = False


class FeaturedContext(InstanceTransitionObserver):
    """
    Represents a specialized context for job instances enriched with features.

    When job instances are added to this context, they are automatically augmented with the features defined
    by the context. The context serves two main purposes:
    1. As a container for job instances.
    2. As an enabler for features for these instances.

    If the context is configured as 'transient', job instances are automatically removed upon reaching their
    termination state.

    The context adheres to the context manager protocol:
    The context must be opened before any job instance is added to it. Upon opening, all provided open hooks
    are executed. The context can be opened either by invoking the `open` method or by using the
    context manager's `__enter__` method.

    Once the context is closed, no further job instances can be added. The closure procedure only initiates
    after the last job instance terminates. As a result, you can trigger the context's closure even
    if some instances are still executing. This can be achieved by either calling the `close` method or
    by using the context manager's `__exit__` mechanism.

    Instance Managers:
        - When an instance is added to the context, it is registered with all instance managers.
        - Conversely, when an instance is removed from the context, it is unregistered from all instance managers.

    Observers:
        - Upon adding an instance to the context, all observers are attached to the instance.
        - The observers are detached from an instance either when the instance reaches its terminal phase
          or when the instance is explicitly removed from the context before its termination.

    Properties:
        instances (list[JobInstance]): A list of job instances managed by the context.
    """

    def __init__(self, instance_managers=(), state_observers=(), output_observers=(), *, transient=False):
        self._instance_managers: Tuple[ManagerFeature[JobInstanceManager]] = tuple(instance_managers)
        self._state_observers: Tuple[ObserverFeature[InstanceTransitionObserver]] = tuple(state_observers)
        self._output_observers: Tuple[ObserverFeature[InstanceOutputObserver]] = tuple(output_observers)
        self._keep_removed = not transient
        self._managed_instances: Dict[str, _ManagedInstance] = {}
        self._ctx_lock = Lock()
        self._opened = False
        self._close_requested = False
        self._closed = False

    @property
    def instances(self):
        """
        Note: The returned list is a mutable copy.

        Returns:
            Return a list of all job instances currently managed by the context.
        """
        with self._ctx_lock:
            return list(managed.instance for managed in self._managed_instances.values())

    def get_instance(self, job_instance_id) -> Optional[JobInstance]:
        """
        Retrieve a specific job instance using its ID.

        Args:
            job_instance_id (JobRunId): The ID of the job instance to retrieve.

        Returns:
            Optional[JobInstance]: The job instance if found, otherwise None.
        """
        with self._ctx_lock:
            managed = self._managed_instances.get(job_instance_id)

        return managed.instance if managed else None

    def __enter__(self):
        self.open()
        return self

    def open(self):
        """
        Open the context and execute all open hooks.
        """
        if self._opened:
            raise InvalidStateError("Managed job context has been already opened")

        self._execute_open_hooks()
        self._opened = True

    def _execute_open_hooks(self):
        for open_hook in (f.open_hook for f in
                          (self._instance_managers + self._state_observers + self._output_observers) if f.open_hook):
            open_hook()

    def add(self, job_instance):
        """
        Add a job instance to the context.

        Args:
            job_instance (JobInstance): The job instance to be added.

        Returns:
            JobInstance: The added job instance.

        Raises:
            InvalidStateError: If the context is not opened or is already closed.
            ValueError: If a job instance with the same ID already exists in the context.
        """
        with self._ctx_lock:
            if not self._opened:
                raise InvalidStateError("Cannot add job instance because the context has not been opened")
            if self._close_requested:
                raise InvalidStateError("Cannot add job instance because the context has been already closed")
            if job_instance.instance_id in self._managed_instances:
                raise ValueError("An instance with this ID has already been added to the context")

            managed_instance = _ManagedInstance(job_instance)
            self._managed_instances[job_instance.instance_id] = managed_instance

        for manager_feat in self._instance_managers:
            manager_feat.component.register_instance(job_instance)

        ctx_observer_priority = 1000
        for state_observer_feat in self._state_observers:
            job_instance.add_observer_phase_transition(state_observer_feat.component, state_observer_feat.priority)
            ctx_observer_priority = max(ctx_observer_priority, state_observer_feat.priority)

        for output_observer_feat in self._output_observers:
            job_instance.add_observer_status(output_observer_feat.component, output_observer_feat.priority)

        # #  TODO optional plugins
        # if cfg.plugins_enabled and cfg.plugins_load:
        #     plugins.register_new_job_instance(job_instance, cfg.plugins_load,
        #                                       plugin_module_prefix=EXT_PLUGIN_MODULE_PREFIX)

        # IMPORTANT:
        #   1. Add observer first and only then check for the termination to prevent release miss by the race condition
        #   2. Priority should be set to be the lowest from all observers, however the current implementation
        #      will work regardless of the priority as the removal of the observers doesn't affect
        #      iteration/notification (`Notification` class)
        job_instance.add_observer_phase_transition(self, ctx_observer_priority + 1)
        if job_instance.job_run_info().run.lifecycle.is_ended:
            self._release_instance(job_instance.metadata.instance_id, not self._keep_removed)

        return job_instance

    def remove(self, job_instance_id) -> Optional[JobInstance]:
        """
        Remove a job instance from the context using its ID.

        Args:
            job_instance_id (JobRunId): The ID of the job instance to remove.

        Returns:
            Optional[JobInstance]: The removed job instance if found, otherwise None.
        """
        return self._release_instance(job_instance_id, True)

    def new_phase(self, job_run: JobRun, previous_phase, new_phase, ordinal):
        """
        DO NOT EXECUTE THIS METHOD! It is part of the internal mechanism.
        """
        if new_phase.run_state == RunState.ENDED:
            self._release_instance(job_run.metadata.instance_id, not self._keep_removed)

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
            job_instance.remove_observer_phase_transition(self)

            for output_observer_feat in self._output_observers:
                job_instance.remove_observer_status(output_observer_feat.component)

            for state_observer_feat in self._state_observers:
                job_instance.remove_observer_phase_transition(state_observer_feat.component)

            for manager_feat in self._instance_managers:
                if manager_feat.unregister_after_termination:
                    manager_feat.component.unregister_instance(job_instance)

            managed_instance.released = True

        if removed:
            for manager_feat in self._instance_managers:
                if not manager_feat.unregister_after_termination:
                    manager_feat.component.unregister_instance(job_instance)

        self._check_close()
        return job_instance

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
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
