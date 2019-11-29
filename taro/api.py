import logging
import os
import socket

from taro import paths

log = logging.getLogger(__name__)


def _create_socket_name(job_instance):
    return job_instance.id + "_api"


class SocketApi:

    def __init__(self, job_instance):
        self.job_instance = job_instance
        self._server: socket = None

    def start(self) -> bool:
        try:
            socket_path = paths.api_socket_path(_create_socket_name(self.job_instance), create=True)
        except FileNotFoundError as e:
            log.error("event=[unable_create_socket_dir] socket_dir=[%s] message=[%s]", e.filename, e)
            return False

        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            self._server.bind(str(socket_path))
            return True
        except PermissionError as e:
            log.error("event=[unable_create_socket] socket=[%s] message=[%s]", socket_path, e)
            return False

    def stop(self):
        if self._server is None:
            return

        socket_name = self._server.getsockname()
        self._server.close()
        if os.path.exists(socket_name):
            os.remove(socket_name)
