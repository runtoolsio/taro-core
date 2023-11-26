class TaroException(Exception):
    pass


class ConfigFileNotFoundError(TaroException, FileNotFoundError):

    def __init__(self, file, search_path=()):
        self.file = file
        self.search_path = search_path

        if search_path:
            message = f"Config file `{file}` not found in the search path: {', '.join([str(dir_) for dir_ in search_path])}"
        else:
            message = f"Config file `{file}` not found"

        super().__init__(message)


class InvalidStateError(TaroException):

    def __init__(self, message: str):
        super().__init__(message)
