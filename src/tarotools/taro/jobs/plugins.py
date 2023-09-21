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

    def __init_subclass__(cls, **kwargs):
        """
        All plugins are registered using subclass registration:
        https://www.python.org/dev/peps/pep-0487/#subclass-registration
        """
        res_name = cls.__module__
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

        plugins_to_init = [name for name in names if name not in initialized]
        if plugins_to_init:
            load_plugins(plugins_to_init)

        for name in plugins_to_init:
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


def load_plugins(modules, *, package=tarotools.plugins, skip_imported=True) -> Dict[str, ModuleType]:
    if not modules:
        raise ValueError("Plugins for discovery not specified")
    discovered_modules = [name for _, name, __ in pkgutil.iter_modules(package.__path__, package.__name__ + ".")]
    log.debug("event=[plugin_modules_discovered] names=[%s]", ",".join(discovered_modules))

    name2module = {}
    for name in modules:
        if skip_imported and name in Plugin.name2subclass.keys():
            continue  # Already imported
        if name not in discovered_modules:
            log.warning("event=[plugin_module_not_found] module=[%s]", name)
            continue

        try:
            module = importlib.import_module(name)
            name2module[name] = module
            log.debug("event=[plugin_module_imported] name=[%s] module=[%s]", name, module)
        except BaseException as e:
            log.exception("event=[plugin_module_invalid] reason=[import_failed] name=[%s] detail=[%s]", name, e)

    return name2module
