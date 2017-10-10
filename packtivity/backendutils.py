import os
import importlib
import yaml
import packtivity.asyncbackends as asyncbackends
import packtivity.syncbackends as syncbackends
import logging

log = logging.getLogger(__name__)


def proxy_from_json(jsondata, best_effort_backend = True, raise_on_unknown = False):
    proxy, backend = None, None
    if jsondata['proxyname'] == 'CeleryProxy':
        from .asyncbackends import CeleryProxy
        proxy = CeleryProxy.fromJSON(jsondata)
        if best_effort_backend:
            _, backend = backend_from_string('celery')
    if jsondata['proxyname'] == 'CeleryProxy':
        from .asyncbackends import CeleryProxy
        proxy = CeleryProxy.fromJSON(jsondata)
        if best_effort_backend:
            _, backend = backend_from_string('celery')

    if jsondata['proxyname'] == 'ForegroundProxy':
        from .asyncbackends import ForegroundProxy
        proxy = ForegroundProxy.fromJSON(jsondata)
        if best_effort_backend:
            _, backend = backend_from_string('foregroundasync')

    if 'PACKTIVITY_ASYNCBACKEND' in os.environ:
        module, _, proxyclass = os.environ['PACKTIVITY_ASYNCBACKEND'].split(':')
        module = importlib.import_module(module)
        proxyclass = getattr(module,proxyclass)
        proxy = proxyclass.fromJSON(jsondata)
        if best_effort_backend:
            _, backend = backend_from_string('fromenv')
    if not proxy and raise_on_unknown:
        raise RuntimeError('unknown proxy type: %s', jsondata['proxyname'])
    if best_effort_backend:
        return proxy, backend
    return proxy

def backend_from_string(backendstring,backendopts = None):
    '''
    creates (a)sync backends from strings
    returns tuple (boolean,backend) where boolean
    specifies whether this is a syncbackend (True) or
    asyncbackend (False)
    '''
    backendopts = backendopts or {}
    ctor_kwargs = os.environ.get('PACKTIVITY_ASYNCBACKEND_OPTS',{})
    if ctor_kwargs:
        ctor_kwargs = yaml.load(open(ctor_kwargs))
        log.info('overriding using envvar opts %s', ctor_kwargs)
        backendopts.update(**ctor_kwargs)
    is_sync, is_async = True, False
    if backendstring == 'defaultsync':
        return is_sync, syncbackends.defaultsyncbackend(**backendopts)
    if backendstring.startswith('multiproc'):
        _,poolsize = backendstring.split(':')
        backendopts.update(poolsize = poolsize)
        backend = asyncbackends.MultiProcBackend(**backendopts)
        return is_async, backend

    if  backendstring == 'foregroundasync':
        backend = asyncbackends.ForegroundBackend(**backendopts)
        return is_async, backend

    if  backendstring == 'ipcluster':
        backend = asyncbackends.IPythonParallelBackend(**backendopts)
        return is_async, backend
    if backendstring == 'celery':
        backend = asyncbackends.CeleryBackend(**backendopts)
        return is_async, backend
    if backendstring == 'fromenv':
        module, backend, _ = os.environ['PACKTIVITY_ASYNCBACKEND'].split(':')
        module = importlib.import_module(module)
        backendclass = getattr(module,backend)
        return is_async, backendclass(**backendopts)
    raise RuntimeError('Unknown Backend %s', backendstring)
