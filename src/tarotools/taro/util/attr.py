"""
This module provides utility functions to work with module or class attributes.
"""

import inspect
from typing import Dict, Any


def get_module_attributes(mod) -> Dict[str, Any]:
    """Return a dictionary of non-special attributes of a module.

    This function retrieves all the non-special attributes of a given module and returns them in a dictionary
    together with their values. Special attributes (those beginning with "__"), uppercase constants, modules, classes,
    functions and imports are excluded from the result.

    Args:
        mod (module): The input module from which to extract the attributes.

    Returns:
        A dictionary where the keys are the names of the non-special attributes
        and the values are the corresponding attribute values.
    """

    attributes = {}

    for attr_name, attr_value in mod.__dict__.items():
        # Ignore dunder (special) methods, uppercase constants, and modules
        if not (attr_name.startswith("__")
                or attr_name.isupper()
                or inspect.ismodule(attr_value)
                or inspect.isclass(attr_value)
                or inspect.isfunction(attr_value)):
            attributes[attr_name] = attr_value

    return attributes
