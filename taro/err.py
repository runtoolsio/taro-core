class TaroException(Exception):
    pass


class ConfigFileNotFoundError(TaroException, FileNotFoundError):
    pass


class InvalidStateError(Exception):

    def __init__(self, message: str):
        super().__init__(message)
