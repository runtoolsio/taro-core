import logging

_root_logger = logging.getLogger()
_root_logger.setLevel(logging.DEBUG)
_formatter = logging.Formatter('%(asctime)s - %(levelname)-5s - %(name)s - %(message)s')

STDOUT_HANDLER = 'stdout-handler'


def init():
    """
    Resetting needed for tests
    """
    _root_logger.disabled = False


def disable():
    logging.disable()
    _root_logger.disabled = True


def is_disabled():
    return _root_logger.disabled


def setup_console(level):
    console_handler = _find_handler_or_register(STDOUT_HANDLER, lambda: logging.StreamHandler())
    console_handler.setLevel(logging.getLevelName(level.upper()))
    console_handler.setFormatter(_formatter)


def get_console_level():
    handler = _find_handler(STDOUT_HANDLER)
    return handler.level if handler else None


def setup_file(level, file):
    file_handler = logging.FileHandler(file)
    file_handler.setLevel(logging.getLevelName(level.upper()))
    file_handler.setFormatter(_formatter)
    _root_logger.addHandler(file_handler)


def _find_handler(name):
    for handler in _root_logger.handlers:
        if handler.name == name:
            return handler

    return None


def _find_handler_or_register(name, factory):
    handler = _find_handler(name)
    if handler:
        return handler

    handler = factory()
    handler.set_name(name)
    _root_logger.addHandler(handler)
    return handler
