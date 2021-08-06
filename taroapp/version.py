import pkg_resources  # part of setuptools

def get():
    version = pkg_resources.require("taro")[0].version
    return version