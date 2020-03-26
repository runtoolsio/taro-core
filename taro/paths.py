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
from typing import Generator

_DEFAULT_CONFIG_FILE = 'default.yaml'
_MINIMAL_CONFIG_FILE = 'minimal.yaml'
_LOG_FILE = 'taro.log'


def _is_root():
    return os.geteuid() == 0


def default_config_file_path() -> Path:
    return config_file_path(_DEFAULT_CONFIG_FILE)


def minimal_config_file_path() -> Path:
    return config_file_path(_MINIMAL_CONFIG_FILE)


def config_file_path(filename) -> Path:
    base_path = Path(__file__).parent
    def_config = base_path / 'config' / filename
    if not def_config.exists():
        raise FileNotFoundError(filename + ' config file not found, corrupted installation?')
    return def_config


"""
https://stackoverflow.com/questions/7567642
S.Lott's answer:
There's usually a multi-step search for the configuration file.
1. Local directory. ./myproject.conf.
2. User's home directory (~user/myproject.conf)
3. A standard system-wide directory (/etc/myproject/myproject.conf)
4. A place named by an environment variable (MYPROJECT_CONF)
"""


def lookup_config_file_path() -> Path:
    """
    1. If non-root user search: ${XDG_CONFIG_HOME}/taro/{config-file}
    2. If not found or root user search: /etc/taro/{config-file}

    :return: config file path
    :raise FileNotFoundError: when config lookup failed
    """

    paths = []

    if not _is_root():
        home_dir = Path.home()
        user_config = home_dir / '.config' / 'taro' / _DEFAULT_CONFIG_FILE
        paths.append(user_config)
        if user_config.exists():
            return user_config

    system_config = Path('/etc/taro') / _DEFAULT_CONFIG_FILE
    paths.append(system_config)
    if system_config.exists():
        return system_config

    raise FileNotFoundError('Config file not found in paths: ' + str([str(config) for config in paths]))


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
