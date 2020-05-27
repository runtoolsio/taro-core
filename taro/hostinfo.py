import configparser
import json
import re
from configparser import ParsingError
import subprocess

import urllib3

from taro import paths

variable_pattern = re.compile('\\{.+\\}')


def read():
    host_info = {}
    host_info_file = configparser.ConfigParser()
    try:
        host_info_file.read(paths.lookup_hostinfo_file())
        if 'const' in host_info_file:
            host_info.update(host_info_file['const'])
        # return {k: _resolve(v) for k, v in host_info_file['config'].items()}
        print(host_info)
    except ParsingError as e:
        raise LookupError('Hostinfo file corrupted') from e
    except FileNotFoundError as e:
        raise LookupError('Hostinfo lookup failed') from e


def _resolve(v):
    if variable_pattern.match(v):
        var = v[1:-1]
        if var == 'ec2.region':
            return _resolve_ec2_region()
        return 'Unknown variable: ' + var
    else:
        return v


def _resolve_ec2_region():
    http = urllib3.PoolManager()
    resp = http.request('GET', 'http://169.254.169.254/latest/dynamic/instance-identity/document', timeout=0.3)
    region = json.loads(resp.data.decode("utf-8"))['region']

    resp = http.request('GET', 'http://169.254.169.254/latest/meta-data/instance-id', timeout=0.3)
    instance_id = resp.data.decode("utf-8")

    tags = subprocess.check_output(
        ['aws', 'ec2', 'describe-tags', '--region', region, '--filters', f'Name=resource-id,Values={instance_id}'])


read()
