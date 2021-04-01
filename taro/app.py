import sys

from taro import cli, cmd, argsconfig, persistence
from taro.persistence.none import PersistenceDisabledError
from taro.util import NestedNamespace


def main_cli():
    main(None)


def main(args):
    """Taro CLI app main function.

    Note: Configuration is setup before execution of all commands although not all commands require it.
          This practice increases safety (in regards with future extensions) and consistency.
          Performance impact is expected to be negligible.

    :param args: CLI arguments
    """
    args = cli.parse_args(args)
    args_ns = NestedNamespace(**vars(args))
    setup_config(args_ns)
    try:
        cmd.run(args_ns)
    except PersistenceDisabledError:
        print('This command cannot be executed with disabled persistence. Enable persistence in config file first.',
              file=sys.stderr)
    finally:
        persistence.close()


def setup_config(args):
    """Load and setup config according to provided CLI arguments

    :param args: CLI arguments
    """
    argsconfig.load_config(args)
    argsconfig.override_config(args)


if __name__ == '__main__':
    main(sys.argv[1:])
