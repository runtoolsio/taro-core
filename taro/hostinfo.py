import configparser
import functools
import json
import logging
import subprocess
from collections import OrderedDict
from configparser import ParsingError
from subprocess import SubprocessError
from typing import Dict

import urllib3
from urllib3.exceptions import HTTPError

from taro import paths

log = logging.getLogger(__name__)


@functools.lru_cache()
def read_hostinfo() -> Dict[str, str]:
    host_info = OrderedDict()
    host_info_file = configparser.ConfigParser()
    host_info_file.optionxform = str
    try:
        host_info_file.read(paths.lookup_hostinfo_file())
    except ParsingError as e:
        raise HostinfoError('Hostinfo file corrupted') from e
    except FileNotFoundError:
        log.debug('event=[no_hostinfo_file]')
        return {}

    if 'const' in host_info_file:
        host_info.update(host_info_file['const'])

    if 'ec2' in host_info_file:
        try:
            _resolve_ec2_section(host_info_file['ec2'], host_info)
        except (HTTPError, SubprocessError) as e:
            log.warning("event=[ec2_hostinfo_error] detail=[{}]".format(e))
            host_info.update({k: '<error>' for k, _ in host_info_file['ec2'].items() if k not in host_info})

    return host_info


def _resolve_ec2_section(mapping, host_info):
    rev_mapping = {v.lower(): k for k, v in mapping.items()}
    http = urllib3.PoolManager()

    resp = http.request('GET', 'http://169.254.169.254/latest/dynamic/instance-identity/document',
                        timeout=0.3, retries=False)
    region = json.loads(resp.data.decode("utf-8"))['region']
    if 'region' in rev_mapping:
        host_info[rev_mapping['region']] = region

    resp = http.request('GET', 'http://169.254.169.254/latest/meta-data/instance-id', timeout=0.3, retries=False)
    instance_id = resp.data.decode("utf-8")
    for k, v in mapping.items():
        if v.lower() in ('instance_id', 'instance-id', 'instanceid'):
            host_info[k] = instance_id

    tags_row = subprocess.check_output(
        ['aws', 'ec2', 'describe-tags', '--region', region, '--filters', f'Name=resource-id,Values={instance_id}'])
    tags = json.loads(tags_row)["Tags"]
    tag2value = {tag['Key'].lower(): tag['Value'] for tag in tags}

    for k, v in mapping.items():
        if k in host_info:  # Already resolved
            continue
        if v.lower().startswith('tag.'):
            tag_name = v.lower()[len('tag.'):]
            host_info[k] = tag2value.get(tag_name, 'Unknown tag: ' + tag_name)
        else:
            host_info[k] = 'Unknown variable: ' + v


class HostinfoError(Exception):

    def __init__(self, message: str):
        super().__init__(message)
