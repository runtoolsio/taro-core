from bottle import request, HTTPError


def query(name: str, *, mandatory=False, default=None, aliases=None, allowed=()):
    if name not in request.query:
        if mandatory:
            raise http_error(412, "Mandatory query parameter '{}' not found".format(name))
        return default
    val = request.query[name]
    if aliases and val in aliases:
        val = aliases[val]
    if allowed and val not in allowed:
        raise http_error(
            412, "Invalid value '{}' for query parameter '{}', allowed values are {}".format(val, name, allowed))
    return val


def query_digit(name: str, *, mandatory=False, default=None):
    val = query(name, mandatory=mandatory, default=default)
    if val:
        if isinstance(val, int):
            return val
        if val.isdigit():
            return int(val)
        else:
            raise http_error(412, "Query parameter '{}' must be a number".format(name))
    else:
        return None


def http_error(status, message):
    return HTTPError(status=status, body='{"message": "' + message + '"}')
