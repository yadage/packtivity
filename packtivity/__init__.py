import pkg_resources
import logging
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

def run_in_env(nametag,environment,command,context):
    env_type = environment['environment_type']
    from handlers.environment_handlers import handlers as env_handlers
    handler = env_handlers[env_type]
    return handler(nametag,environment,context,command)

def prepublish(step,attributes,context):
    '''
    attempts to prepublish output data, returns None if not possible
    '''
    pub = step['publisher']
    if pub['publisher_type'] in ['frompar-pub','consant-pub']:
        return publish(pub,attributes,context)
    return None

class packtivity_callable(object):
    def __init__(self,uniquetag,step,attributes,context):
        self.uniquetag = uniquetag
        self.step = step
        self.attributes = attributes
        self.context = context
        self.published_data = prepublish(self.step,self.attributes,self.context)

    def __call__(self):
        log = logging.getLogger('step_logger_{}'.format(self.uniquetag))
        try:
            job = build_job(self.step['process'],self.attributes)
            run_in_env(self.uniquetag,self.step['environment'],job,self.context)
            if not self.published_data:
                self.published_data = publish(self.step['publisher'],self.attributes,self.context)
            log.debug('%s result: %s',self.uniquetag,self.published_data)
            return self.published_data

        except:
            log.exception('%s raised exception',self.uniquetag)
            raise

def packtivity(uniquetag,step,attributes,context):
    p = packtivity_callable(uniquetag,step,attributes,context)
    return p()
