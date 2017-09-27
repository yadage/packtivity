import os
import importlib
from .posixfs_context import LocalFSState,LocalFSProvider

def load_state(jsondata):
    if jsondata['state_type'] == 'localfs':
        return LocalFSState.fromJSON(jsondata)
    if 'PACKTIVITY_STATEPROVIDER' in os.environ:
        module = importlib.import_module(os.environ['PACKTIVITY_STATEPROVIDER'])
        return module.load_state(jsondata)


    raise TypeError('unknown state type {}'.format(jsondata['state_type']))

def load_provider(jsondata):
    if jsondata == None:
        return None
    if jsondata['state_provider_type'] == 'localfs_provider':
        return LocalFSProvider.fromJSON(jsondata)
