import os
import importlib
import yaml
import logging

import packtivity.asyncbackends as asyncbackends
import packtivity.syncbackends as syncbackends
import packtivity.utils as utils

log = logging.getLogger(__name__)

proxyhandlers, proxyloader = utils.handler_decorator()


@proxyloader("ForegroundProxy")
def foreground_loader(jsondata, deserialization_opts=None, best_effort_backend=False):
    from .asyncbackends import ForegroundProxy

    proxy = ForegroundProxy.fromJSON(jsondata)
    if best_effort_backend:
        _, backend = backend_from_string("foregroundasync")
        return proxy, backend
    return proxy


@proxyloader("ExternalAsyncProxy")
def external_loader(jsondata, deserialization_opts=None, best_effort_backend=False):
    from .asyncbackends import ExternalAsyncProxy

    proxy = ExternalAsyncProxy.fromJSON(jsondata)
    if best_effort_backend:
        _, backend = backend_from_string("externalasync")
        return proxy, backend
    return proxy


@proxyloader("CeleryProxy")
def celery_loader(jsondata, deserialization_opts=None, best_effort_backend=False):
    from .asyncbackends import CeleryProxy

    proxy = CeleryProxy.fromJSON(jsondata)
    if best_effort_backend:
        _, backend = backend_from_string("celery")
        return proxy, backend
    return proxy


@proxyloader("fromenv")
def fromenv_loader(jsondata, deserialization_opts=None, best_effort_backend=False):
    module, _, proxyclass = os.environ["PACKTIVITY_ASYNCBACKEND"].split(":")
    module = importlib.import_module(module)
    proxyclass = getattr(module, proxyclass)
    proxy = proxyclass.fromJSON(jsondata)
    if best_effort_backend:
        _, backend = backend_from_string("fromenv")
        return proxy, backend
    return proxy


@proxyloader("frompython")
def python_loader(jsondata, deserialization_opts=None, best_effort_backend=False):
    _, module, proxyclass = deserialization_opts["proxystring"].split(":")
    module = importlib.import_module(module)
    proxyclass = getattr(module, proxyclass)
    proxyopts = {}
    proxy = proxyclass.fromJSON(jsondata, **proxyopts)
    if best_effort_backend:
        raise RuntimeError("do not know what the backend could be")
    return proxy


def load_proxy(
    jsondata,
    deserialization_opts=None,
    best_effort_backend=True,
    raise_on_unknown=False,
):
    log.debug("load_proxy opts %s", deserialization_opts)
    deserialization_opts = deserialization_opts or {}
    proxy, backend = None, None

    if "proxy" in deserialization_opts:
        proxystring = deserialization_opts.get("proxy", "")
        if proxystring.startswith("py:"):
            proxy = proxyhandlers["frompython"]["default"](
                jsondata, {"proxystring": proxystring}, best_effort_backend
            )

    if "PACKTIVITY_ASYNCBACKEND" in os.environ:
        proxy = proxyhandlers["fromenv"]["default"](
            jsondata, deserialization_opts, best_effort_backend
        )
        if best_effort_backend:
            proxy, backend = proxy

    if jsondata["proxyname"] in list(proxyhandlers.keys()):
        if jsondata["proxyname"] == "PacktivityProxyBase":
            return None  # by definition unserializable

        proxy = proxyhandlers[jsondata["proxyname"]]["default"](
            jsondata, deserialization_opts, best_effort_backend
        )
        if best_effort_backend:
            proxy, backend = proxy

    if not proxy and raise_on_unknown:
        raise RuntimeError("unknown proxy type: %s", jsondata["proxyname"])

    if best_effort_backend:
        return proxy, backend
    return proxy


backends, backend = utils.handler_decorator()
is_sync, is_async = True, False


@backend("foregroundasync")
def foregroundasync_backend(backendstring, backendopts):
    backend = asyncbackends.ForegroundBackend(**backendopts)
    return is_async, backend


@backend("ipcluster")
def ipcluster_backend(backendstring, backendopts):
    backend = asyncbackends.IPythonParallelBackend(**backendopts)
    return is_async, backend


@backend("celery")
def celery_backend(backendstring, backendopts):
    backend = asyncbackends.CeleryBackend(**backendopts)
    return is_async, backend


@backend("multiproc")
def multiproc_backend(backendstring, backendopts):
    _, poolsize = backendstring.split(":")
    backendopts.update(poolsize=poolsize)
    backend = asyncbackends.MultiProcBackend(**backendopts)
    return is_async, backend


@backend("externalasync")
def externalasync_backend(backendstring, backendopts):
    _, externalbackend = backendstring.split(":")
    if externalbackend == "default":
        external = asyncbackends.DefaultExternalJobBackend()
        backend = asyncbackends.ExternalAsyncBackend(job_backend=external)
        return is_async, backend
    else:
        raise NotImplementedError("...")


@backend("py:")
def generic_python_backend(backendstring, backendopts):
    _, module, backend = backendstring.split(":")
    module = importlib.import_module(module)
    backendclass = getattr(module, backend)
    return is_async, backendclass(**backendopts)


@backend("fromenv")
def fromshellenv_backend(backendstring, backendopts):
    module, backend, proxy = os.environ["PACKTIVITY_ASYNCBACKEND"].split(":")
    module = importlib.import_module(module)
    backendclass = getattr(module, backend)
    return is_async, backendclass(**backendopts)


@backend("defaultsync")
def defaultsync_backend(backendstring, backendopts):
    return is_sync, syncbackends.defaultsyncbackend(**backendopts)


def backend_from_string(backendstring, backendopts=None):
    """
    creates (a)sync backends from strings
    returns tuple (boolean,backend) where boolean
    specifies whether this is a syncbackend (True) or
    asyncbackend (False)
    """
    backendopts = backendopts or {}
    ctor_kwargs = os.environ.get("PACKTIVITY_ASYNCBACKEND_OPTS", {})
    if ctor_kwargs:
        ctor_kwargs = yaml.safe_load(open(ctor_kwargs))
        log.info("overriding using envvar opts %s", ctor_kwargs)
        backendopts.update(**ctor_kwargs)

    for k in list(backends.keys()):
        if backendstring.startswith(k):
            return backends[k]["default"](backendstring, backendopts)

    raise RuntimeError("Unknown Backend %s", backendstring, list(backends.keys()))


@backend("kubedirectjob")
def k8s_direct_backend(backendstring, backendopts):
    from .kubernetes import DirectExternalKubernetesBackend

    backend = DirectExternalKubernetesBackend(**backendopts)
    return False, backend
