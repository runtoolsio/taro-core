import json
import logging
import os
import socket
from threading import Thread
from types import coroutine

from taro import paths
from taro.util import iterates

log = logging.getLogger(__name__)

API_FILE_EXTENSION = '.api'


def _create_socket_name(job_instance):
    return job_instance.instance_id + API_FILE_EXTENSION


class Server:

    def __init__(self, job_control):
        self.job_control = job_control
        self._server: socket = None

    def start(self) -> bool:
        try:
            socket_path = paths.socket_path(_create_socket_name(self.job_control), create=True)
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

            if 'req' not in req_body:
                resp_body = {"resp": {"error": "missing_req"}}
            elif 'api' not in req_body['req']:
                resp_body = {"resp": {"error": "missing_req_api"}}
            elif req_body['req']['api'] == '/job':
                resp_body = {"resp": {"code": 200},
                             "data": {"job_id": self.job_control.job_id, "instance_id": self.job_control.instance_id}}
            elif req_body['req']['api'] == '/release':
                if 'data' not in req_body:
                    resp_body = {"resp": {"error": "missing_data"}}
                elif 'wait' not in req_body['data']:
                    resp_body = {"resp": {"error": "missing_data_wait"}}
                else:
                    released = self.job_control.release(req_body.get('data').get('wait'))
                    resp_body = {"resp": {"code": 200},
                                 "data": {"job_id": self.job_control.job_id, "released": released}}
            else:
                resp_body = {"resp": {"error": "unknown_req_api"}}

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

    @coroutine
    def servers(self):
        req_body = '_'
        resp = {}
        client = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        client.bind(client.getsockname())
        try:
            for api_file in paths.socket_files(API_FILE_EXTENSION):
                while True:
                    if resp:
                        req_body = yield resp
                    if not req_body:
                        break
                    try:
                        client.sendto(json.dumps(req_body).encode(), str(api_file))
                        datagram = client.recv(1024)
                        resp = json.loads(datagram.decode())
                    except ConnectionRefusedError:
                        log.warning('event=[dead_socket] socket=[{}]'.format(api_file))  # TODO remove file
                        resp = None  # Ignore and continue with another one
                        break
        finally:
            client.shutdown(socket.SHUT_RDWR)
            client.close()

    @iterates
    def read_job_info(self):
        server = self.servers()
        while True:
            next(server)
            print(server.send({'req': {'api': '/job'}}))

    @iterates
    def release_jobs(self, wait):
        server = self.servers()
        while True:
            next(server)
            resp = server.send({'req': {'api': '/release'}, "data": {"wait": wait}})
            if resp['data']['released']:
                print(resp)
