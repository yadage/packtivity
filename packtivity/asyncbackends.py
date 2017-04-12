from syncbackends import run_packtivity
from syncbackends import prepublish
from syncbackends import packconfig
import multiprocessing
import functools
import sys
import traceback
import os

class PacktivityProxyBase(object):
    '''
    A generic serializable proxy wrapper around a proxy object,
    that is passed in the ctor. Implementations can override details,
    proxyname methods..
    '''
    def __init__(self,proxy):
        self.proxy = proxy

    def details(self):
        return None

    def proxyname(self):
        return 'PacktivityProxyBase'

    def json(self):
        return {
            'proxyname': self.proxyname(),
            'proxydetails': self.details()
        }

class PythonCallableAsyncBackend(object):
    '''
    Basic Base Backends that turn (spec,parameters, context)
    into nullary python callables which then can be submitted
    into python
    '''
    def __init__(self,packconfig_spec):
        self.config = packconfig(**packconfig_spec) if packconfig_spec else packconfig()

    def submit_callable(self,callable):
        raise NotImplementedError('needs implementation')

    def prepublish(self,spec, parameters, context):
        return prepublish(spec, parameters, context, self.config)

    def submit(self, spec, parameters, context):
        nullary = functools.partial(run_packtivity,
            spec = spec,
            parameters = parameters,
            context = context,
            nametag = context.get('nametag','packtivity_async'),
            config = self.config
        )
        return self.submit_callable(nullary)

class MultiProcBackend(PythonCallableAsyncBackend):
    def __init__(self,poolsize, packconfig_spec = None):
        super(MultiProcBackend,self).__init__(packconfig_spec)
        if poolsize == 'auto':
            poolsize = multiprocessing.cpu_count()
        self.pool = multiprocessing.Pool(int(poolsize))

    def submit_callable(self,callable):
        return PacktivityProxyBase(self.pool.apply_async(callable))

    def result(self,resultproxy):
        return resultproxy.proxy.get()

    def ready(self,resultproxy):
        return resultproxy.proxy.ready()

    def successful(self,resultproxy):
        if not self.ready(resultproxy): return False
        return resultproxy.proxy.successful()

    def fail_info(self,resultproxy):
        try:
            self.result(resultproxy)
        except:
            t,v,tb =    sys.exc_info()
            traceback.print_tb(tb)
            return (t,v)

class IPythonParallelBackend(PythonCallableAsyncBackend):
    def __init__(self,client = None, resolve_like_partial = True, packconfig_spec = None):
        from ipyparallel import Client
        super(IPythonParallelBackend,self).__init__(packconfig_spec)
        self.resolve = resolve_like_partial
        self.client = client or Client()
        self.view = self.client.load_balanced_view()

    def submit_callable(self,callable):
        if self.resolve:
            return PacktivityProxyBase(self.view.apply(callable.func,*callable.args,**callable.keywords))
        return PacktivityProxyBase(self.view.apply(callable))

    def result(self,resultproxy):
        return resultproxy.proxy.get()

    def ready(self,resultproxy):
        return resultproxy.proxy.ready()

    def successful(self,resultproxy):
        return resultproxy.proxy.successful()

    def fail_info(self,resultproxy):
        return resultproxy.proxy.exception_info()

try:
    from celery.result import AsyncResult as CeleryAsyncResult
    from celery import Celery
    from celery import shared_task
    default_celeryapp = Celery('defaultapp')
    default_celeryapp.conf.update(
        task_serializer = 'pickle',
        accept_content = ['pickle','json'],
        result_backend = 'redis',
        broker = os.environ.get('PACKTIVITY_CELERY_REDIS_BROKER','redis://localhost:6379')
    )
    @shared_task
    def run_nullary(nullary):
        return nullary()

    class CeleryProxy(PacktivityProxyBase):
        def __init__(self,proxyobj):
            self.proxy = proxyobj

        def proxyname(self):
            return 'CeleryProxy'

        def details(self):
            return {
                'task_id': self.proxy.task_id
            }

        @classmethod
        def fromJSON(cls, data):
            proxy = CeleryAsyncResult(
                data['proxydetails']['task_id']
            )
            return cls(proxy)

    class CeleryBackend(PythonCallableAsyncBackend):
        def __init__(self,app = None, packconfig_spec = None):
            super(CeleryBackend,self).__init__(packconfig_spec)
            self.app = app or default_celeryapp

        def submit_callable(self,callable):
            self.app.set_current()
            return CeleryProxy(run_nullary.apply_async(kwargs = {'nullary': callable}))

        def result(self,resultproxy):
            return resultproxy.proxy.get()

        def ready(self,resultproxy):
            return resultproxy.proxy.ready()

        def successful(self,resultproxy):
            return resultproxy.proxy.successful()

        def fail_info(self,resultproxy):
            try:
                self.result(resultproxy)
            except:
                return sys.exc_info()
except ImportError:
    pass
