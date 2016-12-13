import logging

class packconfig(object):
    def __init__(self,**kwargs):
        self.handler_selection = kwargs

    def get_impl(self,category,handler):
        try:
            return self.handler_selection[category][handler]
        except KeyError:
            return 'default'

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

def publish(publisher,attributes,context, pack_config):
    pub_type   = publisher['publisher_type']
    impl = pack_config.get_impl('publisher',pub_type)
    from handlers.publisher_handlers import handlers as pub_handlers
    handler = pub_handlers[pub_type][impl]
    return handler(publisher,attributes,context)

def prepublish(spec, attributes, context, pack_config):
    '''
    attempts to prepublish output data, returns None if not possible
    '''
    pub = spec['publisher']
    if pub['publisher_type'] in ['frompar-pub','constant-pub']:
        return publish(pub,attributes,context,pack_config)
    return None

def run_packtivity(spec, parameters,context,nametag,config):
    #curry nametag into context
    context['nametag'] = nametag
    log = logging.getLogger('step_logger_{}'.format(nametag))
    try:
        job = build_job(spec['process'],parameters, config)
        run_in_env(spec['environment'],job,context, config)
        return publish(spec['publisher'],parameters,context,config)
    except:
        log.exception('%s raised exception',nametag)
        raise

class defaultsyncbackend(object):
    def __init__(self,packconfig_spec = None):
        self.config = packconfig(**packconfig_spec) if packconfig_spec else packconfig()

    def prepublish(self,spec, parameters, context):
        return prepublish(spec, parameters, context, self.config)

    def run(self,spec,parameters,context, nametag = 'packtivity_syncbackend'):
        return run_packtivity(spec,parameters,context,nametag,self.config)
