import abc
import json
import logging
import os
import socket
from threading import Thread
from types import coroutine

from taro import paths

log = logging.getLogger(__name__)


class SocketServer(abc.ABC):

    def __init__(self, socket_name):
        self._socket_name = socket_name
        self._server: socket = None
        self._stopped = False

    def start(self) -> bool:
        if self._stopped:
            return False
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
        while not self._stopped:
            datagram, client_address = self._server.recvfrom(1024)
            if not datagram:
                break
            req_body = json.loads(datagram)
            resp_body = self.handle(req_body)

            if resp_body:
                if client_address:
                    self._server.sendto(json.dumps(resp_body).encode(), client_address)
                else:
                    log.warning('event=[missing_client_address]')
        log.debug('event=[server_stopped]')

    @abc.abstractmethod
    def handle(self, req_body):
        """
        Handle request and optionally return response
        :return: response body or None if no response
        """

    def stop(self):
        self._stopped = True
        if self._server is None:
            return

        socket_name = self._server.getsockname()
        self._server.shutdown(socket.SHUT_RD)
        self._server.close()
        if os.path.exists(socket_name):
            os.remove(socket_name)


class SocketClient:

    def __init__(self, file_extension: str, bidirectional: bool):
        self._file_extension = file_extension
        self._bidirectional = bidirectional
        self._client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        if bidirectional:
            self._client.bind(self._client.getsockname())

    @coroutine
    def servers(self):
        req_body = '_'  # Dummy initialization to remove warnings
        resp = None
        skip = False
        for api_file in paths.socket_files(self._file_extension):
            while True:
                if not skip:
                    req_body = yield resp
                skip = False  # reset
                if not req_body:
                    break
                try:
                    self._client.sendto(json.dumps(req_body).encode(), str(api_file))
                    if self._bidirectional:
                        datagram = self._client.recv(1024)
                        resp = json.loads(datagram.decode())
                except ConnectionRefusedError:
                    log.warning('event=[dead_socket] socket=[{}]'.format(api_file))  # TODO remove file
                    skip = True  # Ignore this one and continue with another one
                    break

    def close(self):
        self._client.shutdown(socket.SHUT_RDWR)
        self._client.close()
