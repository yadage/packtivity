import logging
import pkg_resources

log = logging.getLogger(__name__)
schemadir = pkg_resources.resource_filename('packtivity','schema')

def publish(publisher,attributes,context):
    pub_type   = publisher['publisher_type']
    from handlers.publisher_handlers import handlers as pub_handlers
    handler = pub_handlers[pub_type]
    return handler(publisher,attributes,context)

def build_job(process,attributes):
    proc_type =  process['process_type']
    from handlers.process_handlers import handlers as proc_handlers
    handler = proc_handlers[proc_type]
    return handler(process,attributes)

def run_in_env(environment,job,context):
    env_type = environment['environment_type']
    from handlers.environment_handlers import handlers as env_handlers
    handler = env_handlers[env_type]
    return handler(environment,context,job)

def prepublish(step,attributes,context):
    '''
    attempts to prepublish output data, returns None if not possible
    '''
    pub = step['publisher']
    if pub['publisher_type'] in ['frompar-pub','constant-pub']:
        return publish(pub,attributes,context)
    return None

class packtivity_callable(object):
    def __init__(self,step,attributes,context):
        '''instantiate packtivity object (a callable with fixed parameters)'''
        self.step = step
        self.attributes = attributes
        self.context = context
        self.published_data = prepublish(self.step,self.attributes,self.context)

    def __call__(self):
        nametag = self.context['nametag']
        log = logging.getLogger('step_logger_{}'.format(nametag))
        try:
            job = build_job(self.step['process'],self.attributes)
            run_in_env(self.step['environment'],job,self.context)
            if not self.published_data:
                self.published_data = publish(self.step['publisher'],self.attributes,self.context)
            log.debug('%s result: %s',nametag,self.published_data)
            return self.published_data

        except:
            log.exception('%s raised exception',nametag)
            raise

def packtivity(step,attributes,context):
    ''''simple blocking packtivity'''
    p = packtivity_callable(step,attributes,context)
    return p()
