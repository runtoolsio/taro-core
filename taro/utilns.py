import functools
from types import SimpleNamespace


class NestedNamespace(SimpleNamespace):

    def get(self, fields: str, default=None, type_=None, allowed=()):
        """Similar to `getattr` but supports chained dot notation for safely accessing nested fields.

        :param fields: field names separated by dot
        :param default: value returned if (possible nested) attribute is not found or is `None`
        :param type_: expected type of the attribute value, an exception is raised if does not match
        :param allowed: list of allowed values
        :returns: a value of the attribute
        """
        return get_attr(self, fields, default, type_, allowed)


def get_attr(obj, fields, default=None, type_=None, allowed=()):
    val = _getattr(obj, fields.split('.'), default, type_)
    if allowed and val not in allowed:
        raise ValueError(f"Value `{val}` for `{fields}` is not in allowed values: {allowed}")
    return val


def _getattr(obj, fields, default, type_):
    attr = getattr(obj, fields[0], default)

    if attr is None:
        return default

    if len(fields) == 1:
        if attr is not None and type_ and not isinstance(attr, type_):
            raise TypeError(f"{attr} is not instance of {type_}")
        return attr
    else:
        return _getattr(attr, fields[1:], default, type_)


# Martijn Pieters' solution below: https://stackoverflow.com/questions/50490856
def wrap_namespace(ob) -> NestedNamespace:
    return _wrap_namespace(ob) or NestedNamespace()


@functools.singledispatch
def _wrap_namespace(ob) -> NestedNamespace:
    """Converts provided dictionary and all dictionaries in its value trees to nested namespace.

    This allows to access nested fields using chained dot notation: value = ns.top.nested
    """
    return ob


@_wrap_namespace.register(dict)
def _wrap_dict(ob):
    return NestedNamespace(**{k: _wrap_namespace(v) for k, v in ob.items()})


@_wrap_namespace.register(list)
def _wrap_list(ob):
    return [_wrap_namespace(v) for v in ob]


def set_attr(obj, fields, value):
    if len(fields) == 1:
        setattr(obj, fields[0], value)
    else:
        set_attr(getattr(obj, fields[0]), fields[1:], value)
