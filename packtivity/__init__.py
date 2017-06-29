import time
import logging
import yadageschemas
import syncbackends
import os
from datetime import datetime
import utils

log = logging.getLogger(__name__)

def prepublish_default(spec,parameters,context):
    backend = syncbackends.defaultsyncbackend()
    return backend.prepublish(spec,parameters,context)

class pack_object(object):
    def __init__(self,spec):
        self.spec = spec

    @classmethod
    def fromspec(cls,*args,**kwargs):
        return cls(utils.load_packtivity(*args,**kwargs))

    def __call__(self, parameters, context,
                 syncbackend = syncbackends.defaultsyncbackend(),
                 asyncbackend = None, asyncwait = False,
                 waitperiod = 0.01, timeout = 43200 ):   #default timeout is 12h

        if syncbackend and not asyncbackend:
            return syncbackend.run(self.spec,parameters,context)
        elif asyncbackend:
            submit_time = datetime.fromtimestamp(time.time())
            proxy = asyncbackend.submit(self.spec, parameters, context)
            if not asyncwait:
                return proxy
            while True:
                if asyncbackend.ready(proxy):
                    return asyncbackend.result(proxy)
                timestamp = datetime.fromtimestamp(time.time())
                if (timestamp - submit_time).seconds > timeout:
                    raise RuntimeError('Timeout!')
                time.sleep(waitperiod)
