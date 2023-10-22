"""
Job Instance Plugin Module
===========================

Purpose:
--------
This module provides plugin mechanisms specifically for job instances. By using plugins, additional features
related to job instance management and monitoring can be introduced, allowing customization based on the
executing environment (plugins installed in the search path) and custom configuration (enabled plugins).

Plugin Representation:
----------------------
Plugins are subclasses of the `Plugin` ABC class and must implement the `JobInstanceManager` interface.

Registration:
-------------
Plugins are automatically registered when their defining module is imported. The `load_modules` utility function
can assist in this process.

Plugin Location:
----------------
By default, the `load_modules` function locates modules in the `tarotools.plugins` namespace subpackage. More details
are available in the official documentation:
https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/#using-namespace-packages

Fetching Plugins:
-----------------
Before retrieving plugins, call the `load_modules` function to ensure all required plugins are registered. The
`Plugin.fetch_plugins` class method can then be used to instantiate the plugins.

Using Plugins:
--------------
Plugins are designed for use by code that manages job instances. The most common practice is to call
`register_instance()` when a new job instance is created and `unregister_instance()` when the job instance is
to be discarded, which typically happens after the instance terminates.

A convenient way to utilize plugins is to use the featured context from the `featurize` module with
the plugins feature set up. This context then handles the registration and un-registration automatically.
"""


import importlib
import logging
import pkgutil
from abc import abstractmethod
from types import ModuleType
from typing import Dict, Type

import tarotools.plugins
from tarotools.taro.jobs.instance import JobInstanceManager

log = logging.getLogger(__name__)


class Plugin(JobInstanceManager):
    _name2subclass: Dict[str, Type] = {}
    _name2plugin: Dict[str, 'Plugin'] = {}

    def __init_subclass__(cls, *, plugin_name=None, **kwargs):
        """
        All plugins are registered using subclass registration:
        https://www.python.org/dev/peps/pep-0487/#subclass-registration
        """
        res_name = plugin_name or cls.__module__.split('.')[-1]
        cls._name2subclass[res_name] = cls
        log.debug("event=[plugin_registered] name=[%s] class=[%s]", res_name, cls)

    @classmethod
    def fetch_plugins(cls, names, *, cached=False) -> Dict[str, 'Plugin']:
        """
        Instantiates and returns registered plugins based on the provided names.
        A plugin gets registered once its defining module has been imported.

        If the `cached` parameter is set to `True`, the behavior alters slightly:
         1. All fetched plugins are stored in a cache.
         2. If a plugin is already in the cache, it's not instantiated again but instead returned from the cache.

        Note: If the cache is used, it is the code client's responsibility to execute `close_all`
              when all cached plugins are no longer needed to ensure proper resource cleanup.

        Args:
            names (List[str]): Names of the plugins to be fetched.
            cached (bool): Determines if the listed plugins should be cached or retrieved from the cache.

        Returns:
            Dict[str, 'Plugin']: Dictionary mapping plugin names to their instances.
        """
        if not names:
            raise ValueError("Plugins not specified")

        if cached:
            initialized = {name: cls._name2plugin[name] for name in names if name in cls._name2plugin}
        else:
            initialized = {}

        for name in (name for name in names if name not in initialized):
            try:
                plugin_cls = Plugin._name2subclass[name]
            except KeyError:
                log.warning("event=[plugin_not_found] name=[%s]", name)
                continue
            try:
                plugin = plugin_cls()
                initialized[name] = plugin
                log.debug("event=[plugin_created] name=[%s] plugin=[%s]", name, plugin)
                if cached:
                    cls._name2plugin[name] = plugin
            except PluginDisabledError as e:
                log.warning("event=[plugin_disabled] name=[%s] detail=[%s]", name, e)
            except Exception as e:
                log.warning("event=[plugin_instantiation_failed] name=[%s] detail=[%s]", name, e)

        return initialized

    @classmethod
    def close_all(cls):
        for name, plugin in cls._name2plugin.items():
            try:
                plugin.close()
            except Exception as e:
                log.warning("event=[plugin_closing_failed] name=[%s] plugin=[%s] detail=[%s]", name, plugin, e)

    @abstractmethod
    def unregister_after_termination(self):
        """
        Determines if the manager of the plugin should always unregister an instance immediately after its termination.
        This is useful for plugins which operate only with active instances.

        Returns:
            bool: `True` if the instance should be unregistered post-termination, otherwise `False`.
        """
        pass

    @abstractmethod
    def close(self):
        """
        Releases resources held by the plugin when it's no longer needed. (Or ignore when not required...)
        """
        pass


class PluginDisabledError(Exception):
    """
    This exception can be thrown from plugin's init method to signalise that there is a condition preventing
    the plugin to work. It can be an initialization error, missing configuration, etc.
    """

    def __init__(self, message: str):
        super().__init__(message)


def load_modules(modules, *, package=tarotools.plugins) -> Dict[str, ModuleType]:
    """
    Utility function to ensure all plugins are registered before use.
    Users of the plugins API should call this before utilizing any plugin.

    Args:
        modules (List[str]): Modules where plugins are defined.
        package (ModuleType, optional): Base package for plugins. Defaults to `tarotools.plugins` namespace sub-package.
    """

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
