import time
import logging
import yadageschemas
import syncbackends
from datetime import datetime

log = logging.getLogger(__name__)

def load_pack(spec,toplevel,schemasource = yadageschemas.schemadir,validate = True):
    #in case that spec is a json reference string, we will treat it as such
    #if it's just a filename, this should not affect it...
    spec   = yadageschemas.load(
            {'$ref':spec},
            toplevel,
            'packtivity/packtivity-schema',
            schemadir = schemasource,
            validate = validate,
            initialload = False
    )
    return spec

def prepublish_default(spec,parameters,context):
    backend = syncbackends.defaultsyncbackend()
    return backend.prepublish(spec,parameters,context)

class pack_object(object):
    def __init__(self,spec):
        self.spec = spec

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
