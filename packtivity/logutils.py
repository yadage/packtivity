import logging
import importlib
import contextlib

LOGFORMAT = "%(asctime)s | %(name)20.20s | %(levelname)6s | %(message)s"
formatter = logging.Formatter(LOGFORMAT)


def get_base_loggername(metadata):
    return "pack.{}".format(metadata["name"])


def get_topic_loggername(metadata, topic):
    return "pack.{}.{}".format(metadata["name"], topic)


def default_logging_handlers(exec_config, log, metadata, state, topic):
    if topic == "step":
        sh = logging.StreamHandler()
        sh.setLevel(getattr(logging, exec_config.stream_loglevel()))
        sh.setFormatter(formatter)
        log.addHandler(sh)

    # Now that we have  place to store meta information we put a file based logger in place
    # to log at DEBUG
    if state and state.metadir:
        logname = "{}/{}.{}.log".format(state.metadir, metadata["name"], topic)
        fh = logging.FileHandler(logname)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        log.addHandler(fh)
        log.info("starting file logging for topic: %s", topic)


@contextlib.contextmanager
def setup_logging_topic(exec_config, metadata, state, topic, return_logger=False):
    """
    a context manager for logging
    it is a context in order to be able to clean up the logging after it's not needed
    if many loggers and handlers that open resources are created at some point these
    resoures may dry up. that's why we need a specific end point.
    The logger can be recreated multiple times
    """

    log = logging.getLogger(get_topic_loggername(metadata, topic))
    log.setLevel(logging.DEBUG)
    log.propagate = False

    if exec_config.disable_logging():
        pass
    else:
        if not log.handlers:
            customhandlers = exec_config.custom_logging_handler()
            if customhandlers:
                module, func = customhandlers.split(":")
                m = importlib.import_module(module)
                f = getattr(m, func)
                f(log, metadata, state, topic)
            else:
                default_logging_handlers(exec_config, log, metadata, state, topic)

    yield log if return_logger else None

    for h in log.handlers:
        h.close()
        log.removeHandler(h)
