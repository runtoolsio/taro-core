import logging
from threading import Lock
from typing import List

from taro import cfg, JobInstance, ExecutionStateObserver, JobInfo
from taro.err import InvalidStateError
from taro.jobs import lock, plugins
from taro.jobs.api import Server
from taro.jobs.events import StateDispatcher, OutputDispatcher
from taro.jobs.runner import RunnerJobInstance

log = logging.getLogger(__name__)

EXT_PLUGIN_MODULE_PREFIX = plugins.DEF_PLUGIN_MODULE_PREFIX


class ManagedJobContext(ExecutionStateObserver):

    def __init__(self,
                 api_factory=Server,
                 state_dispatcher_factory=StateDispatcher,
                 output_dispatcher_factory=OutputDispatcher):
        self._api = api_factory()
        self._state_dispatcher = state_dispatcher_factory()
        self._output_dispatcher = output_dispatcher_factory()
        self._closeable = (self._api, self._state_dispatcher, self._output_dispatcher)
        self._managed_jobs = []
        self._managed_jobs_lock = Lock()
        self._opened = False
        self._closed = False

    @property
    def managed_jobs(self) -> List[JobInstance]:
        with self._managed_jobs:
            return list(self._managed_jobs)

    def __enter__(self):
        if self._opened:
            raise InvalidStateError("Managed job context has been already opened")
        self._opened = True

        if self._api:
            api_started = self._api.start()  # Starts a new thread
            if not api_started:
                raise InvalidStateError("API server failed to start")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._managed_jobs_lock:
            if self._closed:
                return

            self._closed = True
            if not self._managed_jobs:
                self._close()

    def create_job(self, job_id, execution, state_locker=lock.default_state_locker(), *,
                   no_overlap=False, depends_on=None, pending_value=None, **params) -> JobInstance:
        if not self._opened:
            raise InvalidStateError("Cannot create job because the context has not been opened")
        if self._closed:
            raise InvalidStateError("Cannot create job because the context has been already closed")

        # TODO instance_id and plugins
        job_instance = \
            RunnerJobInstance(job_id, execution, state_locker,
                              pending_value=pending_value, no_overlap=no_overlap, depends_on=depends_on, **params)
        if self._state_dispatcher:
            job_instance.add_state_observer(self._state_dispatcher, 100)
        if self._output_dispatcher:
            job_instance.add_output_observer(self._output_dispatcher, 100)
        if self._api:
            self._api.add_job_instance(job_instance)

        if cfg.plugins:
            plugins.register_new_job_instance(job_instance, cfg.plugins, plugin_module_prefix=EXT_PLUGIN_MODULE_PREFIX)

        job_instance.add_state_observer(self, 1000)  # Must be notified last because it is used to close job resources
        self._managed_jobs.append(job_instance)

        return job_instance

    def state_update(self, job_info: JobInfo):
        if job_info.lifecycle.state.is_terminal():
            self._close_job(job_info.id)

    def _close_job(self, job_instance_id):
        with self._managed_jobs_lock:
            job_instance = next(j for j in self._managed_jobs if j.id == job_instance_id)
            self._managed_jobs.remove(job_instance)
            close = self._closed and len(self._managed_jobs) == 0

        job_instance.remove_state_observer(self._state_dispatcher)
        job_instance.remove_output_observer(self._output_dispatcher)
        self._api.remove_job_instance(job_instance)

        if close:
            self._close()

    def _close(self):
        for closeable in self._closeable:
            # noinspection PyBroadException
            try:
                closeable.close()
            except BaseException:
                log.exception("event=[failed_to_close_resource] resource=[%s]", closeable)
