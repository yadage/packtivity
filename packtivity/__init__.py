import logging
import pkg_resources

log = logging.getLogger(__name__)
schemadir = pkg_resources.resource_filename('packtivity','schema')

class packconfig(object):
    def __init__(self,**kwargs):
        self.handler_selection = kwargs

    def get_impl(self,category,handler):
        try:
            return self.handler_selection[category][handler]
        except KeyError:
            return 'default'

def publish(publisher,attributes,context, pack_config):
    pub_type   = publisher['publisher_type']
    impl = pack_config.get_impl('publisher',pub_type)
    from handlers.publisher_handlers import handlers as pub_handlers
    handler = pub_handlers[pub_type][impl]
    return handler(publisher,attributes,context)

def build_job(process,attributes,pack_config):
    '''
    takes a process description and builds a job out of it using a handler.
    '''
    proc_type =  process['process_type']
    impl = pack_config.get_impl('process',proc_type)
    from handlers.process_handlers import handlers as proc_handlers
    handler = proc_handlers[proc_type][impl]
    return handler(process,attributes)

def run_in_env(environment,job,context,pack_config):
    '''
    takes a built job and runs it blockingly in the environment with
    the state context attached
    '''
    env_type = environment['environment_type']
    impl = pack_config.get_impl('environment',env_type)
    from handlers.environment_handlers import handlers as env_handlers
    handler = env_handlers[env_type][impl]
    return handler(environment,context,job)

def prepublish(step,attributes,context,config = None):
    '''
    attempts to prepublish output data, returns None if not possible
    '''
    pub = step['publisher']
    if pub['publisher_type'] in ['frompar-pub','constant-pub']:
        return publish(pub,attributes,context,config or packconfig())
    return None

class packtivity_callable(object):
    def __init__(self,step,attributes,context, config = None):
        '''instantiate packtivity object (a callable with fixed parameters)'''
        self.config = config or packconfig()
        self.step = step
        self.attributes = attributes
        self.context = context
        self.published_data = prepublish(self.step,self.attributes,self.context,self.config)

    def __call__(self):
        nametag = self.context['nametag']
        log = logging.getLogger('step_logger_{}'.format(nametag))
        try:
            job = build_job(self.step['process'],self.attributes,self.config)
            run_in_env(self.step['environment'],job,self.context,self.config)
            if not self.published_data:
                self.published_data = publish(self.step['publisher'],self.attributes,self.context,self.config)
            log.debug('%s result: %s',nametag,self.published_data)
            return self.published_data
        except:
            log.exception('%s raised exception',nametag)
            raise

def packtivity(step,attributes,context):
    ''''simple blocking packtivity'''
    p = packtivity_callable(step,attributes,context)
    return p()
