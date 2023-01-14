import abc
import logging
import os
import socket
from enum import Enum, auto
from threading import Thread
from types import coroutine
from typing import List, NamedTuple, Optional, Sequence

from taro import paths

RECV_BUFFER_LENGTH = 65536

log = logging.getLogger(__name__)


class SocketServer(abc.ABC):

    def __init__(self, socket_name, *, allow_ping=False):
        self._socket_name = socket_name
        self._allow_ping = allow_ping
        self._server: socket = None
        self._serving_thread = Thread(target=self.serve, name='Thread-ApiServer')
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
            self._serving_thread.start()
            return True
        except PermissionError as e:
            log.error("event=[unable_create_socket] socket=[%s] message=[%s]", socket_path, e)
            return False

    def serve(self):
        log.debug('event=[server_started]')
        while not self._stopped:
            datagram, client_address = self._server.recvfrom(RECV_BUFFER_LENGTH)
            if not datagram:
                break

            req_body = datagram.decode()
            if self._allow_ping and req_body == 'ping':
                resp_body = 'pong'
            else:
                resp_body = self.handle(req_body)

            if resp_body:
                if client_address:
                    encoded = resp_body.encode()
                    try:
                        self._server.sendto(encoded, client_address)
                    except OSError as e:
                        if e.errno == 90:
                            log.error(f"event=[server_response_payload_too_large] length=[{len(encoded)}]")
                        raise e
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

    def close(self):
        self.stop()

        if self._server is None:
            return

        socket_name = self._server.getsockname()
        try:
            self._server.shutdown(socket.SHUT_RD)
            self._server.close()
        finally:
            if os.path.exists(socket_name):
                os.remove(socket_name)

    def wait(self):
        self._serving_thread.join()

    def close_and_wait(self):
        self.close()
        self.wait()


class Error(Enum):
    TIMEOUT = auto()


class ServerResponse(NamedTuple):
    server_id: str
    response: Optional[str]
    error: Error = None


class SocketClient:

    def __init__(self, file_extension: str, bidirectional: bool, *, timeout=2):
        self._file_extension = file_extension
        self._bidirectional = bidirectional
        self._client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        if bidirectional:
            self._client.bind(self._client.getsockname())
            self._client.settimeout(timeout)
        self.timed_out_sockets = []
        self.dead_sockets = []

    @coroutine
    def servers(self, include=()):
        """

        :param include: server IDs exact match filter
        :return: response if bidirectional
        :raises PayloadTooLarge: when request payload is too large
        """
        req_body = '_'  # Dummy initialization to remove warnings
        resp = None
        skip = False
        for api_file in paths.socket_files(self._file_extension):
            server_id = api_file.stem
            if (api_file in self.dead_sockets) or (include and server_id not in include):
                continue
            while True:
                if not skip:
                    req_body = yield resp
                skip = False  # reset
                if not req_body:
                    break  # next(this) called -> proceed to the next server

                encoded = req_body.encode()
                try:
                    self._client.sendto(encoded, str(api_file))
                    if self._bidirectional:
                        datagram = self._client.recv(RECV_BUFFER_LENGTH)
                        resp = ServerResponse(server_id, datagram.decode())
                except TimeoutError:
                    log.warning('event=[socket_timeout] socket=[{}]'.format(api_file))
                    self.timed_out_sockets.append(api_file)
                    resp = ServerResponse(server_id, None, Error.TIMEOUT)
                except ConnectionRefusedError:  # TODO what about other errors?
                    log.warning('event=[dead_socket] socket=[{}]'.format(api_file))
                    self.dead_sockets.append(api_file)
                    skip = True  # Ignore this one and continue with another one
                    break
                except OSError as e:
                    if e.errno == 90:
                        raise PayloadTooLarge(len(encoded))
                    raise e

    def communicate(self, req, include=()) -> List[ServerResponse]:
        server = self.servers(include=include)
        responses = []
        while True:
            try:
                next(server)
                responses.append(server.send(req))  # StopIteration is raised from this function if last socket is dead
            except StopIteration:
                break
        return responses

    def close(self):
        self._client.shutdown(socket.SHUT_RDWR)
        self._client.close()


class PayloadTooLarge(Exception):
    """
    This exception is thrown when the operating system rejects sent datagram due to its size.
    """

    def __init__(self, payload_size):
        super().__init__("Datagram payload is too large: " + str(payload_size))


def clean_dead_sockets(file_extensions: Sequence[str]):
    for ext in file_extensions:
        client = SocketClient(ext, True)
        responses = client.communicate('ping')
        print(responses)
