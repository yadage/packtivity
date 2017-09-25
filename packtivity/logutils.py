import os
import logging
import importlib
import contextlib

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def get_base_loggername(metadata):
    return 'packtivity_logger_{}'.format(metadata['name'])

def get_topic_loggername(metadata,topic):
    return 'packtivity_logger_{}.{}'.format(metadata['name'],topic)

def default_logging_handlers(log,metadata,state,topic):
    if topic == 'step':
        sh  = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        sh.setFormatter(formatter)
        log.addHandler(sh)

    # Now that we have  place to store meta information we put a file based logger in place
    # to log at DEBUG
    logname = '{}/{}.{}.log'.format(state.metadir,metadata['name'],topic)
    fh  = logging.FileHandler(logname)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    log.addHandler(fh)

@contextlib.contextmanager
def setup_logging_topic(metadata,state,topic,return_logger = False):
    '''
    a context manager for logging
    it is a context in order to be able to clean up the logging after it's not needed
    if many loggers and handlers that open resources are created at some point these
    resoures may dry up. that's why we need a specific end point. 
    The logger can be recreated multiple times
    '''
    log = logging.getLogger(get_topic_loggername(metadata,topic))
    log.setLevel(logging.DEBUG)
    log.propagate = False

    if not log.handlers:
        customhandlers = os.environ.get('PACKTIVITY_LOGGING_HANDLER')
        if customhandlers:
            module,func = customhandlers.split(':')
            m = importlib.import_module(module)
            f = getattr(m,func)
            f(log,metadata,state,topic)
        else:
            default_logging_handlers(log,metadata,state,topic)    

    yield log if return_logger else None

    for h in log.handlers:
        h.close()
        log.removeHandler(h)

