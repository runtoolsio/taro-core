import json
import logging
import os
import socket
from threading import Thread

from taro import paths

log = logging.getLogger(__name__)

API_FILE_EXTENSION = '.api'


def _create_socket_name(job_instance):
    return job_instance.id + API_FILE_EXTENSION


class Server:

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
            if not client_address:
                log.warning('event=[missing_client_address]')
                continue
            req_body = json.loads(datagram)
            if req_body.get('api') == '/jobs':
                resp_body = {'job_id': self.job_instance.job_id, 'instance_id': self.job_instance.id}
                self._server.sendto(json.dumps(resp_body).encode(), client_address)

    def stop(self):
        if self._server is None:
            return

        socket_name = self._server.getsockname()
        self._server.shutdown(socket.SHUT_RD)
        self._server.close()
        if os.path.exists(socket_name):
            os.remove(socket_name)


class Client:

    def __init__(self):
        self._client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)

    def read_job_info(self):
        self._client.bind(self._client.getsockname())
        api_dir = paths.api_socket_dir(False)
        api_files = (entry for entry in api_dir.iterdir() if entry.is_socket() and API_FILE_EXTENSION == entry.suffix)
        for api_file in api_files:
            self._client.connect(str(api_file))
            req_body = {'api': '/jobs'}
            self._client.send(json.dumps(req_body).encode())
            datagram = self._client.recv(1024)
            print(datagram.decode())
        self._client.shutdown(socket.SHUT_RDWR)
        self._client.close()
