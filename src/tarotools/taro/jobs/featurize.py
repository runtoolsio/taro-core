import logging
from dataclasses import dataclass
from threading import Lock
from typing import Optional, Tuple, Callable, TypeVar, Generic

from tarotools.taro import InstanceStateObserver, JobInst
from tarotools.taro.err import InvalidStateError
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

    def __init__(self):
        self._instance_managers = []
        self._state_observers = []
        self._output_observers = []
        # TODO job_context_listeners

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

    def build(self) -> 'FeaturedContext':
        instance_managers = []
        for factory, open_hook, close_hook, unregister_terminated in self._instance_managers:
            feature = factory()
            cnv_open_hook = _convert_hook(feature, open_hook)
            cnv_close_hook = _convert_hook(feature, close_hook)

            instance_managers.append(ManagerFeature(feature, cnv_open_hook, cnv_close_hook, unregister_terminated))

        state_observers = _create_observer_features(self._state_observers)
        output_observers = _create_observer_features(self._output_observers)

        return FeaturedContext(instance_managers, state_observers, output_observers)


class FeaturedContext(InstanceStateObserver):

    def __init__(self, instance_managers=(), state_observers=(), output_observers=()):
        self._instance_managers: Tuple[ManagerFeature[JobInstanceManager]] = tuple(instance_managers)
        self._state_observers: Tuple[ObserverFeature[InstanceStateObserver]] = tuple(state_observers)
        self._output_observers: Tuple[ObserverFeature[InstanceOutputObserver]] = tuple(output_observers)
        self._managed_jobs = []
        self._managed_jobs_lock = Lock()
        self._opened = False
        self._closed = False

    @property
    def managed_jobs(self):
        with self._managed_jobs:
            return list(self._managed_jobs)

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

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._managed_jobs_lock:
            if self._closed:
                return

            self._closed = True
            if not self._managed_jobs:
                self._close()

    def _close(self):
        self._execute_close_hooks()

    def _execute_close_hooks(self):
        for close_hook in (f.close_hook for f in
                           (self._instance_managers + self._state_observers + self._output_observers) if f.close_hook):
            # noinspection PyBroadException
            try:
                close_hook()
            except BaseException:
                log.exception("event=[close_hook_error] hook=[%s]", close_hook)

    def add(self, job_instance):
        with self._managed_jobs_lock:
            if not self._opened:
                raise InvalidStateError("Cannot add job instance because the context has not been opened")
            if self._closed:
                raise InvalidStateError("Cannot add job instance because the context has been already closed")
            self._managed_jobs.append(job_instance)

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

        # Must be notified last because it is used to close job resources
        job_instance.add_state_observer(self, ctx_observer_priority + 1)

        return job_instance

    def new_instance_state(self, job_inst: JobInst, previous_state, new_state, changed):
        if new_state.in_phase(ExecutionPhase.TERMINAL):
            self._close_job(job_inst.id)

    def _close_job(self, job_instance_id):
        with self._managed_jobs_lock:
            job_instance = next(j for j in self._managed_jobs if j.id == job_instance_id)
            self._managed_jobs.remove(job_instance)
            close = self._closed and len(self._managed_jobs) == 0

        for output_observer_feat in self._output_observers:
            job_instance.remove_output_observer(output_observer_feat.feature)

        for state_observer_feat in self._state_observers:
            job_instance.remove_state_observer(state_observer_feat.feature)

        for manager_feat in self._instance_managers:
            manager_feat.feature.unregister_instance(job_instance)

        job_instance.remove_state_observer(self)

        if close:
            self._close()
