from bottle import request, HTTPError


def query_multi(name: str, *, mandatory=False, default=(), aliases=None, allowed=()):
    if name not in request.query:
        if mandatory:
            raise http_error(412, "Mandatory query parameter '{}' not found".format(name))
        return default
    values = request.query.getall(name)
    if aliases:
        values = [aliases.get(val, val) for val in values]
    if allowed and not all(val in allowed for val in values):
        allowed_val = ", ".join(allowed)
        raise http_error(
            412, "Invalid value '{}' for query parameter '{}'. Allowed values: {}".format(values, name, allowed_val))
    return values


def query(name: str, *, mandatory=False, default=None, aliases=None, allowed=()):
    values = query_multi(name, mandatory=mandatory, default=default, aliases=aliases, allowed=allowed)
    if values == default:
        return values

    return values[0]


def query_digit(name: str, *, mandatory=False, default=None):
    val = query(name, mandatory=mandatory, default=default)
    if val is None:
        return None

    if isinstance(val, int):
        return val
    if val.isdigit():
        return int(val)
    else:
        raise http_error(412, "Query parameter '{}' must be a number".format(name))


def http_error(status, message):
    return HTTPError(status=status, body='{"message": "' + message + '"}')
