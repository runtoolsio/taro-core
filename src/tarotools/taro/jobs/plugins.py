import abc
import importlib
import logging
import pkgutil
from types import ModuleType
from typing import Dict

from tarotools.taro.jobs.inst import JobInstance

log = logging.getLogger(__name__)

DEF_PLUGIN_MODULE_PREFIX = 'taro_'


# TODO plugin collisions
class PluginBase(abc.ABC):
    name2subclass = {}
    name2plugin = {}

    def __init_subclass__(cls, *, plugin_name=None, **kwargs):
        """
        All plugins are registered using subclass registration:
        https://www.python.org/dev/peps/pep-0487/#subclass-registration
        """

        super().__init_subclass__(**kwargs)
        res_name = plugin_name or cls.__module__
        cls.name2subclass[res_name] = cls
        log.debug("event=[plugin_registered] name=[%s] class=[%s]", res_name, cls)

    @classmethod
    def load_plugins(cls, ext_prefix, names, *, reload=True) -> Dict[str, 'PluginBase']:
        if not names:
            raise ValueError("Plugins not specified")

        new_plugins = [name for name in names if name not in cls.name2plugin]
        if not new_plugins and not reload:
            return  # All plugins already loaded TODO do not return None

        not_discovered = [name for name in new_plugins if name not in cls.name2subclass]
        if not_discovered:
            discover_ext_plugins(ext_prefix, not_discovered)

        name2plugin = {}
        for name in names:
            if not reload and name in cls.name2plugin:
                name2plugin[name] = cls.name2plugin[name]
                continue

            try:
                plugin_cls = PluginBase.name2subclass[name]
            except KeyError:
                log.warning("event=[plugin_not_found] name=[%s]", name)
                continue
            try:
                plugin = plugin_cls()
                name2plugin[name] = plugin
                log.debug("event=[plugin_created] name=[%s] plugin=[%s]", name, plugin)
            except PluginDisabledError as e:
                log.warning("event=[plugin_disabled] name=[%s] detail=[%s]", name, e)
            except BaseException as e:
                log.warning("event=[plugin_instantiation_failed] name=[%s] detail=[%s]", name, e)

        cls.name2plugin = name2plugin
        return name2plugin

    @abc.abstractmethod
    def new_job_instance(self, job_instance: JobInstance):
        """
        New job instance created.
        """


class PluginDisabledError(Exception):
    """
    This exception can be thrown from plugin's init method to signalise that there is a condition preventing
    the plugin to work. It can be an initialization error, missing configuration, etc.
    """

    def __init__(self, message: str):
        super().__init__(message)


def discover_ext_plugins(ext_prefix, names, skip_imported=True) -> Dict[str, ModuleType]:
    if not names:
        raise ValueError("Plugins for discovery not specified")
    discovered_names = [name for finder, name, is_pkg in pkgutil.iter_modules() if name.startswith(ext_prefix)]
    log.debug("event=[ext_plugin_modules_discovered] names=[%s]", ",".join(discovered_names))

    name2module = {}
    for name in names:
        if skip_imported and name in PluginBase.name2subclass.keys():
            continue  # Already imported
        if name not in discovered_names:
            log.warning("event=[ext_plugin_module_not_found] module=[%s]", name)
            continue

        try:
            module = importlib.import_module(name)
            name2module[name] = module
            log.debug("event=[ext_plugin_module_imported] name=[%s] module=[%s]", name, module)
        except BaseException as e:
            log.exception("event=[ext_plugin_module_invalid] reason=[import_failed] name=[%s] detail=[%s]", name, e)

    return name2module


def register_new_job_instance(job_instance, plugins, *, plugin_module_prefix=DEF_PLUGIN_MODULE_PREFIX, reload=False):
    PluginBase.load_plugins(plugin_module_prefix, plugins, reload=reload)  # Load plugins if not yet loaded
    for plugin in PluginBase.name2plugin.values():  # May contain other plugins loaded before
        try:
            plugin.new_job_instance(job_instance)
        except BaseException as e:
            log.warning("event=[plugin_failed] reason=[exception_on_new_job_instance] detail=[%s]", e)
