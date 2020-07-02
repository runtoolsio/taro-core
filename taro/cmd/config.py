from taro import cnf, cli


def run(args):
    if args.config_action == cli.ACTION_CONFIG_SHOW:
        cnf.print_config(args)
