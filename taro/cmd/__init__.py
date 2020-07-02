import importlib


def run(args):
    action = args.action.replace('-', '_')
    cmd = importlib.import_module('.' + action, __name__)
    cmd.run(args)
