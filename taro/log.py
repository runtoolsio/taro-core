import argparse
import logging


def configure(args):
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)-5s - %(name)s - %(message)s')

    for log in args.log:
        log_type = log[0].lower()
        level = log[1] if len(log) > 1 else None

        if 'file' == log_type:
            pass
        else:
            if len(log) > 2:
                raise argparse.ArgumentError(None,
                                             "Argument --log with value <{}> may contain only one additional value: "
                                             "<level>, unknown: {}".format(log_type, ' '.join(log[2:])))
            if 'stdout' == log_type:
                _configure_console(root_logger, formatter, level)


def _configure_console(root, formatter, level):
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level or logging.WARN)
    console_handler.setFormatter(formatter)
    root.addHandler(console_handler)
