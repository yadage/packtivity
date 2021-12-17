import multiprocessing
import functools
import sys
import traceback
import os
import json
import logging

from .syncbackends import (
    prepublish,
    packconfig,
    ExecutionConfig,
    run_packtivity,
    run_in_env,
    finalize_inputs,
    finalize_outputs,
    acquire_job_env,
    publish,
)
from packtivity.statecontexts import load_state
from . import datamodel as _datamodel

log = logging.getLogger(__name__)


class PacktivityProxyBase(object):
    """
    A generic serializable proxy wrapper around a proxy object,
    that is passed in the ctor. Implementations can override details,
    proxyname methods..
    """

    def __init__(self, proxy=None, details=None):
        if proxy:
            self.proxy = proxy
        self._details = details

    def details(self):
        return self._details

    def set_details(self, details):
        self._details = details

    def proxyname(self):
        return "PacktivityProxyBase"

    def json(self):
        return {"proxyname": self.proxyname(), "proxydetails": self.details()}


class ExternalAsyncProxy(PacktivityProxyBase):
    def __init__(self, jobproxy, spec, statedata, pardata, resultdata=None):
        self.jobproxy = jobproxy
        self.resultdata = resultdata
        self.spec = spec
        self.statedata = statedata
        self.pardata = pardata

    def details(self):
        prox = None
        try:
            json.dumps(self.jobproxy)
            prox = self.jobproxy
        except TypeError:
            pass
        return {
            "resultdata": self.resultdata,
            "jobproxy": prox,
            "spec": self.spec,
            "statedata": self.statedata,
            "pardata": self.pardata,
        }

    def proxyname(self):
        return "ExternalAsyncProxy"

    @classmethod
    def fromJSON(cls, data):
        if not data["proxydetails"]["jobproxy"]:
            raise RuntimeError("not external backend proxy saved during serialization")
        return cls(**data["proxydetails"])


class ExternalAsyncMixin(object):
    def __init__(self, **kwargs):
        self.job_backend = kwargs["job_backend"]
        self.deserialization_opts = kwargs.get("deserialization_opts", {})
        self.datamodel = _datamodel

    def make_external_job(self, spec, parameters, state, metadata):
        raise NotImplementedError

    def prepublish(self, spec, parameters, state):
        return None

    def submit(self, spec, parameters, state, metadata=None):
        job = self.make_external_job(spec, parameters, state, metadata)
        jobproxy = self.job_backend.submit(job)
        return ExternalAsyncProxy(jobproxy, spec, state.json(), parameters.json())

    def ready(self, resultproxy):
        return self.job_backend.ready(resultproxy.jobproxy)

    def successful(self, resultproxy):
        return self.job_backend.successful(resultproxy.jobproxy)

    def fail_info(self, resultproxy):
        return self.job_backend.fail_info(resultproxy.jobproxy)


class RemoteResultMixin(object):
    def __init__(self, **kwargs):
        self.resultbackend = kwargs["resultbackend"]
        self.datamodel = _datamodel

    def result(self, resultproxy):
        state = load_state(resultproxy.statedata, self.deserialization_opts)
        if resultproxy.resultdata is not None:
            return self.datamodel.create(resultproxy.resultdata, state.datamodel)
        log.debug(
            "retrieving result for jobid: %s at %s",
            resultproxy.jobproxy["job_id"],
            resultproxy.jobproxy["resultjson"],
        )
        return self.datamodel.create(
            self.resultbackend.get(resultproxy.jobproxy["resultjson"]), state.datamodel
        )


class ExternalAsyncBackend(ExternalAsyncMixin):
    def __init__(self, **kwargs):
        ExternalAsyncMixin.__init__(self, **kwargs)
        self.config = packconfig(**kwargs.get("config", {}))

    def prepublish(self, spec, parameters, state):
        return prepublish(spec, parameters, state, self.config)

    def make_external_job(self, spec, parameters, state, metadata):
        parameters, state = finalize_inputs(parameters, state)
        job, env = acquire_job_env(spec, parameters, state, metadata, self.config)
        return {"job": job, "env": env, "state": state, "metadata": metadata}

    def result(self, resultproxy):
        state = load_state(resultproxy.statedata, self.deserialization_opts)

        if resultproxy.resultdata is not None:
            return self.datamodel.create(resultproxy.resultdata, state.datamodel)

        parameters = self.datamodel.create(resultproxy.pardata, state.datamodel)
        pubdata = publish(resultproxy.spec["publisher"], parameters, state, self.config)
        log.info("publishing data: %s", pubdata)
        pubdata = finalize_outputs(pubdata)
        resultproxy.resultdata = pubdata.json()
        return pubdata


class DefaultExternalJobBackend(object):
    def __init__(self, config=None):
        self.pool = multiprocessing.Pool(1)
        self.config = packconfig(**config) if config else packconfig()

    def submit(self, job):
        nullary = functools.partial(
            run_in_env,
            job=job["job"],
            environment=job["env"],
            state=job["state"],
            metadata=job["metadata"],
            pack_config=self.config,
        )
        return self.pool.apply_async(nullary)

    def ready(self, resultproxy):
        return resultproxy.ready()

    def successful(self, resultproxy):
        return resultproxy.successful()

    def fail_info(self, resultproxy):
        try:
            self.result(resultproxy)
        except:
            t, v, tb = sys.exc_info()
            traceback.print_tb(tb)
            return (t, v)


class PythonCallableAsyncBackend(object):
    """
    Basic Base Backends that turn (spec,parameters, state)
    into nullary python callables which then can be submitted
    into python
    """

    def __init__(self, config):
        config = config or {}
        self.exec_config = ExecutionConfig(config.pop("exec", None))
        self.pack_config = packconfig(**config) if config else packconfig()

    def submit_callable(self, callable):
        raise NotImplementedError("needs implementation")

    def prepublish(self, spec, parameters, state):
        return prepublish(spec, parameters, state, self.pack_config)

    def submit(self, spec, parameters, state, metadata=None):
        nullary = functools.partial(
            run_packtivity,
            spec=spec,
            parameters=parameters,
            state=state,
            metadata=metadata or {"name": "packtivity"},
            pack_config=self.pack_config,
            exec_config=self.exec_config,
        )
        return self.submit_callable(nullary)


class MultiProcBackend(PythonCallableAsyncBackend):
    def __init__(self, poolsize, packconfig_spec=None):
        super(MultiProcBackend, self).__init__(packconfig_spec)
        if poolsize == "auto":
            poolsize = multiprocessing.cpu_count()

        log.info("configured pool size to %s", poolsize)
        self.pool = multiprocessing.Pool(int(poolsize))

    def submit_callable(self, callable):
        return PacktivityProxyBase(self.pool.apply_async(callable))

    def result(self, resultproxy):
        return resultproxy.proxy.get()

    def ready(self, resultproxy):
        return resultproxy.proxy.ready()

    def successful(self, resultproxy):
        if not self.ready(resultproxy):
            return False
        return resultproxy.proxy.successful()

    def fail_info(self, resultproxy):
        try:
            self.result(resultproxy)
        except:
            t, v, tb = sys.exc_info()
            traceback.print_tb(tb)
            return (t, v)


class ForegroundProxy(PacktivityProxyBase):
    def __init__(self, resultdata, datamodel, success, details=None):
        super(ForegroundProxy, self).__init__(details=details)
        self.resultdata = resultdata
        self.datamodel = datamodel
        self.success = success

    def proxyname(self):
        return "ForegroundProxy"

    def details(self):
        d = super(ForegroundProxy, self).details() or {}
        d.update(
            resultdata=self.resultdata, datamodel=self.datamodel, success=self.success
        )
        return d

    @classmethod
    def fromJSON(cls, data):
        return cls(
            data["proxydetails"]["resultdata"],
            data["proxydetails"]["datamodel"],
            data["proxydetails"]["success"],
            data["proxydetails"],
        )


class ForegroundBackend(PythonCallableAsyncBackend):
    def __init__(self, config=None):
        super(ForegroundBackend, self).__init__(config)
        self.datamodel = _datamodel

    def submit(self, spec, parameters, state, metadata=None):

        result = run_packtivity(
            spec,
            parameters,
            state,
            metadata=metadata or {"name": "packtivity"},
            pack_config=self.pack_config,
            exec_config=self.exec_config,
        )
        return ForegroundProxy(
            result.json(), state.datamodel if state else None, success=True
        )

    def result(self, resultproxy):
        return self.datamodel.create(resultproxy.resultdata, resultproxy.datamodel)

    def ready(self, resultproxy):
        return True

    def successful(self, resultproxy):
        if not self.ready(resultproxy):
            return False
        return self.result(resultproxy)

    def fail_info(self, resultproxy):
        pass


class IPythonParallelBackend(PythonCallableAsyncBackend):
    def __init__(self, client=None, resolve_like_partial=True, packconfig_spec=None):
        from ipyparallel import Client

        super(IPythonParallelBackend, self).__init__(packconfig_spec)
        self.resolve = resolve_like_partial
        self.client = client or Client()
        self.view = self.client.load_balanced_view()

    def submit_callable(self, callable):
        if self.resolve:
            return PacktivityProxyBase(
                self.view.apply(callable.func, *callable.args, **callable.keywords)
            )
        return PacktivityProxyBase(self.view.apply(callable))

    def result(self, resultproxy):
        return resultproxy.proxy.get()

    def ready(self, resultproxy):
        return resultproxy.proxy.ready()

    def successful(self, resultproxy):
        return resultproxy.proxy.successful()

    def fail_info(self, resultproxy):
        return resultproxy.proxy.exception_info()


try:
    from celery.result import AsyncResult as CeleryAsyncResult
    from celery import Celery
    from celery import shared_task

    default_celeryapp = Celery("defaultapp")
    default_celeryapp.conf.update(
        task_serializer="pickle",
        result_serializer="pickle",
        accept_content=["pickle", "json"],
        broker_url=os.environ.get(
            "PACKTIVITY_CELERY_REDIS_BROKER", "redis://localhost:6379"
        ),
        result_backend=os.environ.get(
            "PACKTIVITY_CELERY_REDIS_BROKER", "redis://localhost:6379"
        ),
        result_expires=False,
        broker_transport_options={
            "visibility_timeout": os.environ.get(
                "PACKTIVITY_CELERY_VISIBILITY_TIMEOUT", 86400
            )
        },
        worker_prefetch_multiplier=1,
    )

    @shared_task
    def run_nullary(nullary):
        if os.environ.get("PACKTIVITY_CELERY_GLOBAL_NAMETAG") == "true":
            nullary.keywords["metadata"]["name"] = run_nullary.request.id
        return nullary()

    class CeleryProxy(PacktivityProxyBase):
        def __init__(self, proxyobj):
            self.proxy = proxyobj

        def proxyname(self):
            return "CeleryProxy"

        def details(self):
            return {"task_id": self.proxy.task_id}

        @classmethod
        def fromJSON(cls, data):
            proxy = CeleryAsyncResult(data["proxydetails"]["task_id"])
            return cls(proxy)

    class CeleryBackend(PythonCallableAsyncBackend):
        def __init__(self, app=None, packconfig_spec=None):
            super(CeleryBackend, self).__init__(packconfig_spec)
            self.app = app or default_celeryapp
            self.disable_sync = os.environ.get("PACKTIVITY_CELERY_DISABLE_SYNC", "true")

        def submit_callable(self, callable):
            self.app.set_current()
            return CeleryProxy(run_nullary.apply_async(kwargs={"nullary": callable}))

        def result(self, resultproxy):
            return resultproxy.proxy.get(disable_sync_subtasks=self.disable_sync)

        def ready(self, resultproxy):
            return resultproxy.proxy.ready()

        def successful(self, resultproxy):
            return resultproxy.proxy.successful()

        def fail_info(self, resultproxy):
            try:
                self.result(resultproxy)
            except:
                return sys.exc_info()


except ImportError:
    pass
