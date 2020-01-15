def get_attr(obj, fields, default=None):
    return _getattr(obj, fields.split('.'), default)


def _getattr(obj, fields, default):
    attr = getattr(obj, fields[0], default)

    if attr is None:
        return default

    if len(fields) == 1:
        return attr
    else:
        return _getattr(attr, fields[1:], default)


def set_attr(obj, fields, value):
    if len(fields) == 1:
        setattr(obj, fields[0], value)
    else:
        set_attr(getattr(obj, fields[0]), fields[1:], value)


def prime(func):
    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        next(cr)
        return cr

    return start
