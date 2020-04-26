import abc
import importlib
import logging
import pkgutil
from types import ModuleType
from typing import Dict

from taro.job import JobControl

log = logging.getLogger(__name__)


class PluginBase(abc.ABC):
    name2subclass = {}

    def __init_subclass__(cls, name=None, **kwargs):
        """
        All plugins are registered using subclass registration:
        https://www.python.org/dev/peps/pep-0487/#subclass-registration
        """

        super().__init_subclass__(**kwargs)
        cls.name2subclass[name or cls.__module__] = cls
        log.debug("event=[plugin_registered] name=[%s] class=[%s]", name, cls)

    @staticmethod
    def create_plugins(ext_prefix, names) -> Dict[str, 'PluginBase']:
        discover_ext_plugins(ext_prefix, names)

        name2plugin = {}
        for name in names:
            try:
                plugin_cls = PluginBase.name2subclass[name]
            except KeyError:
                log.warning("event=[plugin_not_found] name=[%s]", name)
                continue
            try:
                plugin = plugin_cls()
                name2plugin[name] = plugin
                log.debug("event=[plugin_created] name=[%s] plugin=[%s]", name, plugin)
            except BaseException as e:
                log.warning("event=[plugin_instantiation_failed] name=[%s] detail=[%s]", name, e)

        return name2plugin

    @abc.abstractmethod
    def new_job_instance(self, job_instance: JobControl):
        """
        New job instance created.
        """


def discover_ext_plugins(ext_prefix, names, skip_imported=True) -> Dict[str, ModuleType]:
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
