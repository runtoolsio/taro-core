import importlib
import logging
import pkgutil
from inspect import signature
from types import ModuleType
from typing import Dict, Type

from taro.job import ExecutionStateObserver

log = logging.getLogger(__name__)


class PluginBase:
    name2subclass = {}

    def __init_subclass__(cls, name=None, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.name2subclass[name or cls.__module__] = cls

    @staticmethod
    def create_plugins(ext_prefix, names):
        pass


def discover_plugins(ext_prefix, names) -> Dict[str, ModuleType]:
    discovered = [name for finder, name, is_pkg in pkgutil.iter_modules() if name.startswith(ext_prefix)]
    log.debug("event=[plugin_discovered] plugins=[%s]", ",".join(discovered))

    name2module = {}
    for name in names:
        if name not in discovered:
            log.warning("event=[plugin_not_found] plugin=[%s]", name)
            continue

        try:
            module = importlib.import_module(name)
            name2module[name] = module
            log.debug("event=[plugin_module_imported] plugin=[%s] module=[%s]", name, module)
        except BaseException as e:
            log.exception("event=[invalid_plugin] plugin=[%s] reason=[%s]", name, e)

    return name2module
