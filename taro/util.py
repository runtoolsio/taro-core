def get_attr(obj, fields, none=None):
    return _getattr(obj, fields.split('.'), none)


def _getattr(obj, fields, none):
    attr = getattr(obj, fields[0])

    if attr is None:
        return none

    if len(fields) == 1:
        return attr
    else:
        return _getattr(attr, fields[1:], none)


def set_attr(obj, fields, value):
    if len(fields) == 1:
        setattr(obj, fields[0], value)
    else:
        set_attr(getattr(obj, fields[0]), fields[1:], value)
