"""
Followed conventions:
 - https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html
 - https://refspecs.linuxfoundation.org/FHS_3.0/fhs/ch03s15.html

Discussion:
 - https://askubuntu.com/questions/14535/whats-the-local-folder-for-in-my-home-directory

 TODO Read XDG env variables: https://wiki.archlinux.org/index.php/XDG_Base_Directory
"""

import getpass
import os
from pathlib import Path
from typing import Generator, List

CONFIG_FILE = 'taro.yaml'
_HOSTINFO_FILE = 'hostinfo'
_LOG_FILE = 'taro.log'


def _is_root():
    return os.geteuid() == 0


def default_config_file_path() -> Path:
    return config_file_path(CONFIG_FILE)


def config_file_path(filename) -> Path:
    base_path = Path(__file__).parent
    def_config = base_path / 'config' / filename
    if not def_config.exists():
        raise FileNotFoundError(filename + ' config file not found, corrupted installation?')
    return def_config


def lookup_config_file():
    return lookup_config_file_path(CONFIG_FILE)


def lookup_hostinfo_file():
    return lookup_config_file_path(_HOSTINFO_FILE)


def config_file_search_path(*, exclude_cwd=False) -> List[Path]:
    """Specifies the folders in which the program should look for configuration files:
    1. Search in the current working directory first
    2. If non-root user search: ${XDG_CONFIG_HOME}/taro/{config-file}
    3. If not found or root user search: /etc/taro/{config-file}

    Related discussion: https://stackoverflow.com/questions/1024114
    :return: list of directories for configuration file lookup
    """
    search_path = []
    if not exclude_cwd:
        search_path.append(Path.cwd())
    if not _is_root():  # TODO Use this also for root user?
        search_path.append(Path.home() / '.config' / 'taro')
    search_path.append(Path('/etc/taro'))  # TODO Should be /etc/xdg instead?

    return search_path


def lookup_config_file_path(file) -> Path:
    """Returns config found in the search path
    :return: config file path
    :raise FileNotFoundError: when config lookup failed
    """
    for config_dir in config_file_search_path():
        config = config_dir / file
        if config.exists():
            return config

    raise FileNotFoundError(f'Config file {file} not found in the search path: '
                            + ",".join([str(dir_) for dir_ in config_file_search_path()]))


def log_file_path(create: bool) -> Path:
    """
    1. Root user: /var/log/taro/{log-file}
    2. Non-root user: ${XDG_CACHE_HOME}/taro/{log-file}

    :param create: create path directories if not exist
    :return: log file path
    """

    if _is_root():
        path = Path('/var/log/taro')
    else:
        home = Path.home()
        path = home / '.cache' / 'taro'

    if create:
        path.mkdir(parents=True, exist_ok=True)

    return path / 'taro.log'


"""
XDG_RUNTIME_DIR most likely not suitable for api files as it can gets deleted even when nohup/screen is used:
  https://xdg.freedesktop.narkive.com/kxtUbAAM/xdg-runtime-dir-when-is-a-user-logged-out

https://unix.stackexchange.com/questions/88083/idiomatic-location-for-file-based-sockets-on-debian-systems
goldilocks's answer:
They are commonly found in /tmp or a subdirectory thereof.
You will want to use a subdirectory if you want to restrict access via permissions, since /tmp is world readable.
Note that /run, and all of the other directories mentioned here except /tmp, are only writable by root.
For a system process, this is fine, but if the application may be run by a non-privileged user,
you either want to use /tmp or create a permanent directory somewhere and set permissions on that,
or use a location in the user's $HOME.
"""


def socket_dir(create: bool) -> Path:
    """
    1. Root user: /run/taro
    2. Non-root user: /tmp/taro_${USER} (An alternative may be: ${HOME}/.cache/taro)

    :param create: create path directories if not exist
    :return: directory path for unix domain sockets
    :raises FileNotFoundError: when path cannot be created (only if create == True)
    """

    if _is_root():
        path = Path('/run/taro')
    else:
        path = Path(f"/tmp/taro_{getpass.getuser()}")

    if create:
        path.mkdir(mode=0o700, exist_ok=True)

    return path


def socket_path(socket_name: str, create: bool) -> Path:
    """
    1. Root user: /run/taro/{socket-name}
    2. Non-root user: /tmp/taro_${USER}/{socket-name} (An alternative may be: ${HOME}/.cache/taro/{socket-name})

    :param socket_name: socket file name
    :param create: create path directories if not exist
    :return: unix domain socket path
    :raises FileNotFoundError: when path cannot be created (only if create == True)
    """

    return socket_dir(create) / socket_name


def socket_files(file_extension: str) -> Generator[Path, None, None]:
    s_dir = socket_dir(False)
    if not s_dir.exists():
        return (_ for _ in ())
    return (entry for entry in s_dir.iterdir() if entry.is_socket() and file_extension == entry.suffix)


def sqlite_db_path(create: bool) -> Path:
    """
    1. Root user: /var/lib/taro/{db-file}
    2. Non-root user: ${XDG_DATA_HOME}/taro/{db-file}

    :param create: create path directories if not exist
    :return: db file path
    """

    if _is_root():
        path = Path('/var/lib/taro')
    else:
        home = Path.home()
        path = home / '.local' / 'share' / 'taro'

    if create:
        path.mkdir(parents=True, exist_ok=True)

    return path / 'taro.db'
