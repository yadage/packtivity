import os
import importlib

import asyncbackends
import syncbackends

def proxy_from_json(jsondata, best_effort_backend = True):
    if jsondata['proxyname'] == 'CeleryProxy':
        from asyncbackends import CeleryProxy
        proxy = CeleryProxy.fromJSON(jsondata)
        _, backend = backend_from_string('celery')
    if jsondata['proxyname'] == 'CeleryProxy':
        from asyncbackends import CeleryProxy
        proxy = CeleryProxy.fromJSON(jsondata)
        _, backend = backend_from_string('celery')

    if jsondata['proxyname'] == 'ForegroundProxy':
        from asyncbackends import ForegroundProxy
        proxy = ForegroundProxy.fromJSON(jsondata)
        _, backend = backend_from_string('foregroundasync')

    if 'PACKTIVITY_ASYNCBACKEND' in os.environ:
        module, _, proxyclass = os.environ['PACKTIVITY_ASYNCBACKEND'].split(':')
        module = importlib.import_module(module)
        proxyclass = getattr(module,proxyclass)
        proxy = proxyclass.fromJSON(jsondata)
        _, backend = backend_from_string('fromenv')
    if best_effort_backend:
        return proxy, backend
    return proxy

def backend_from_string(backendstring):
    '''
    creates (a)sync backends from strings
    returns tuple (boolean,backend) where boolean
    specifies whether this is a syncbackend (True) or
    asyncbackend (False)
    '''
    is_sync, is_async = True, False
    if backendstring == 'defaultsync':
        return is_sync, syncbackends.defaultsyncbackend()
    if backendstring.startswith('multiproc'):
        _,poolsize = backendstring.split(':')
        backend = asyncbackends.MultiProcBackend(poolsize = poolsize)
        return is_async, backend

    if  backendstring == 'foregroundasync':
        backend = asyncbackends.ForegroundBackend()
        return is_async, backend

    if  backendstring == 'ipcluster':
        backend = asyncbackends.IPythonParallelBackend()
        return is_async, backend
    if backendstring == 'celery':
        backend = asyncbackends.CeleryBackend()
        return is_async, backend
    if backendstring == 'fromenv':
        module, backend, _ = os.environ['PACKTIVITY_ASYNCBACKEND'].split(':')
        module = importlib.import_module(module)
        backendclass = getattr(module,backend)
        return is_async, backendclass()
    raise RuntimeError('Unknown Backend')