import abc
import json
import logging
import os
import socket
from threading import Thread

from taro import paths

log = logging.getLogger(__name__)


class SocketServer(abc.ABC):

    def __init__(self, socket_name):
        self._socket_name = socket_name
        self._server: socket = None

    def start(self) -> bool:
        try:
            socket_path = paths.socket_path(self._socket_name, create=True)
        except FileNotFoundError as e:
            log.error("event=[unable_create_socket_dir] socket_dir=[%s] message=[%s]", e.filename, e)
            return False

        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            self._server.bind(str(socket_path))
            Thread(target=self.serve, name='Thread-ApiServer').start()
            return True
        except PermissionError as e:
            log.error("event=[unable_create_socket] socket=[%s] message=[%s]", socket_path, e)
            return False

    def serve(self):
        log.debug('event=[server_started]')
        while True:
            datagram, client_address = self._server.recvfrom(1024)
            if not datagram:
                log.debug('event=[server_stopped]')
                break
            req_body = json.loads(datagram)
            resp_body = self.handle(req_body)

            if resp_body:
                if client_address:
                    self._server.sendto(json.dumps(resp_body).encode(), client_address)
                else:
                    log.warning('event=[missing_client_address]')

    @abc.abstractmethod
    def handle(self, req_body):
        """
        Handle request and optionally return response
        :return: response body or None if no response
        """

    def stop(self):
        if self._server is None:
            return

        socket_name = self._server.getsockname()
        self._server.shutdown(socket.SHUT_RD)
        self._server.close()
        if os.path.exists(socket_name):
            os.remove(socket_name)
