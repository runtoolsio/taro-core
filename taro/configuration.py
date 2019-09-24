import yaml

from taro import paths


def read_config():
    config_file_path = paths.config_file_path()
    with open(config_file_path, 'r') as stream:
        return yaml.safe_load(stream)
