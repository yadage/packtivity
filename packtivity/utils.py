import os
import errno

def handler_decorator():
    handlers = {}
    def decorator(name, implementation = 'default'):
        def wrap(func):
            handlers.setdefault(name,{})[implementation] = func
        return wrap
    return handlers,decorator

def mkdir_p(path):
    #http://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

import asyncbackends
import syncbackends

def proxy_from_json(jsondata, best_effort_backend = True):
    if jsondata['proxyname'] == 'CeleryProxy':
        from asyncbackends import CeleryProxy
        proxy = CeleryProxy.fromJSON(jsondata)
        if best_effort_backend:
            _, backend = backend_from_string('celery')
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
    if backendstring.startswith('ipcluster'):
        backend = asyncbackends.IPythonParallelBackend()
        return is_async, backend
    if backendstring.startswith('celery'):
        backend = asyncbackends.CeleryBackend()
        return is_async, backend
