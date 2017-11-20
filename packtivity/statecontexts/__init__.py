import os
import importlib
import logging
from .posixfs_context import LocalFSState

log = logging.getLogger(__name__)

def load_state(jsondata,deserialization_opts = None):
    log.debug('load_state opts %s', deserialization_opts)
    deserialization_opts = deserialization_opts or {}
    if 'state' in deserialization_opts:
        statestring = deserialization_opts.get('state','')
        if statestring.startswith('py:'):
            _, module, stateclass = statestring.split(':')
            module = importlib.import_module(module)
            stateclass = getattr(module,stateclass)
            stateopts = {}
            return stateclass.fromJSON(jsondata,**stateopts)
    if 'PACKTIVITY_STATEPROVIDER' in os.environ:
        module = importlib.import_module(os.environ['PACKTIVITY_STATEPROVIDER'])
        return module.load_state(jsondata)
    if jsondata['state_type'] == 'localfs':
        return LocalFSState.fromJSON(jsondata)
    raise TypeError('unknown state type {}'.format(jsondata['state_type']))
