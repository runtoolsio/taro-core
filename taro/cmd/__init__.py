import importlib

from taro import NestedNamespace


def run(args):
    action = args.action.replace('-', '_')
    cmd = importlib.import_module('.' + action, __name__)
    args_ns = NestedNamespace(**vars(args))
    cmd.run(args_ns)
