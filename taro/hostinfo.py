import re
import configparser
from configparser import ParsingError

from taro import paths

variable_pattern = re.compile('\\{.+\\}')


def read():
    try:
        with open(paths.lookup_hostinfo_file(), 'r') as f:
            # Add [config] to satisfy ConfigParser section requirements
            hostinfo_str = '[config]\n' + f.read()
            hostinfo_cnf = configparser.ConfigParser()
            # Prevent converting option names to lower case
            hostinfo_cnf.optionxform = str
            hostinfo_cnf.read_string(hostinfo_str)
            return {k: _resolve(v) for k, v in hostinfo_cnf['config'].items()}
    except ParsingError as e:
        raise LookupError('Hostinfo file corrupted') from e
    except FileNotFoundError as e:
        raise LookupError('Hostinfo lookup failed') from e


def _resolve(v):
    if variable_pattern.match(v):
        var = v[1:-1]
        return var
    else:
        return v

print(read())
