from taro import socket
from taro.jobs import api


def run(args):
    socket.clean_dead_sockets([api.API_FILE_EXTENSION])
