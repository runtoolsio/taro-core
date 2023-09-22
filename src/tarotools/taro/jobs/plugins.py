import importlib
import logging
import pkgutil
from abc import abstractmethod
from types import ModuleType
from typing import Dict, Type

import tarotools.plugins
from tarotools.taro.jobs.inst import JobInstanceManager

log = logging.getLogger(__name__)


class Plugin(JobInstanceManager):
    name2subclass: Dict[str, Type] = {}
    name2plugin: Dict[str, 'Plugin'] = {}

    def __init_subclass__(cls, *, plugin_name=None, **kwargs):
        """
        All plugins are registered using subclass registration:
        https://www.python.org/dev/peps/pep-0487/#subclass-registration
        """
        res_name = plugin_name or cls.__module__.split('.')[-1]
        cls.name2subclass[res_name] = cls
        log.debug("event=[plugin_registered] name=[%s] class=[%s]", res_name, cls)

    @classmethod
    def fetch_plugins(cls, names, *, cached=False) -> Dict[str, 'Plugin']:
        if not names:
            raise ValueError("Plugins not specified")

        if cached:
            initialized = {name: cls.name2plugin[name] for name in names if name in cls.name2plugin}
        else:
            initialized = {}

        for name in (name for name in names if name not in initialized):
            try:
                plugin_cls = Plugin.name2subclass[name]
            except KeyError:
                log.warning("event=[plugin_not_found] name=[%s]", name)
                continue
            try:
                plugin = plugin_cls()
                initialized[name] = plugin
                log.debug("event=[plugin_created] name=[%s] plugin=[%s]", name, plugin)
                if cached:
                    cls.name2plugin[name] = plugin
            except PluginDisabledError as e:
                log.warning("event=[plugin_disabled] name=[%s] detail=[%s]", name, e)
            except BaseException as e:
                log.warning("event=[plugin_instantiation_failed] name=[%s] detail=[%s]", name, e)

        return initialized

    @abstractmethod
    def close(self):
        pass


class PluginDisabledError(Exception):
    """
    This exception can be thrown from plugin's init method to signalise that there is a condition preventing
    the plugin to work. It can be an initialization error, missing configuration, etc.
    """

    def __init__(self, message: str):
        super().__init__(message)


def load_modules(modules, *, package=tarotools.plugins) -> Dict[str, ModuleType]:
    if not modules:
        raise ValueError("Modules for discovery not specified")

    discovered_modules = [name for _, name, __ in pkgutil.iter_modules(package.__path__, package.__name__ + ".")]
    log.debug("event=[plugin_modules_discovered] names=[%s]", ",".join(discovered_modules))

    name2module = {}
    for name in modules:
        full_name = f"{package.__name__}.{name}"
        if full_name not in discovered_modules:
            log.warning("event=[plugin_module_not_found] module=[%s]", name)
            continue

        try:
            module = importlib.import_module(full_name)
            name2module[name] = module
            log.debug("event=[plugin_module_imported] name=[%s] module=[%s]", module, module)
        except BaseException as e:
            log.exception("event=[plugin_module_invalid] reason=[import_failed] name=[%s] detail=[%s]", name, e)

    return name2module
