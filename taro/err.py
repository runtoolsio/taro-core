class TaroException(Exception):
    pass


class ConfigFileNotFoundError(TaroException, FileNotFoundError):

    def __init__(self, file, search_path):
        message = f"Config file {file} not found in the search path: {', '.join([str(dir_) for dir_ in search_path])}"
        super().__init__(message)


class InvalidStateError(Exception):

    def __init__(self, message: str):
        super().__init__(message)
