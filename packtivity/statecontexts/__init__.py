import os
import importlib
import logging
from .posixfs_context import LocalFSState,LocalFSProvider

log = logging.getLogger(__name__)

def load_state(jsondata):
    if jsondata['state_type'] == 'localfs':
        return LocalFSState.fromJSON(jsondata)
    if 'PACKTIVITY_STATEPROVIDER' in os.environ:
        module = importlib.import_module(os.environ['PACKTIVITY_STATEPROVIDER'])
        return module.load_state(jsondata)
    raise TypeError('unknown state type {}'.format(jsondata['state_type']))

def load_provider(jsondata,deserialization_opts = None):
    log.debug('load_provider opts %s', deserialization_opts)
    deserialization_opts = deserialization_opts or {}
    if jsondata == None:
        return None
    if jsondata['state_provider_type'] == 'localfs_provider':
        return LocalFSProvider.fromJSON(jsondata)
    if 'PACKTIVITY_STATEPROVIDER' in os.environ:
        module = importlib.import_module(os.environ['PACKTIVITY_STATEPROVIDER'])
        return module.load_provider(jsondata)
    raise TypeError('unknown provider type {}'.format(jsondata['state_provider_type']))
