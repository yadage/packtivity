import yaml
import os
import jq
import copy

import packtivity.logutils as logutils
from packtivity.handlers import enable_plugins
enable_plugins()

class packconfig(object):
    def __init__(self,**kwargs):
        self.handler_selection = kwargs
        fromenv = os.environ.get('PACKTIVITY_SYNCCONFIGFILE',None)
        if fromenv:
            override = yaml.load(open(fromenv))
            self.handler_selection.update(**override)

    def get_impl(self,category,handler):
        try:
            return self.handler_selection[category][handler]
        except KeyError:
            return 'default'

def build_job(process,parameters,state,pack_config):
    '''
    takes a process template and builds a job out of it using a handler.
    '''
    proc_type =  process['process_type']
    impl = pack_config.get_impl('process',proc_type)
    from .handlers.process_handlers import handlers as proc_handlers
    handler = proc_handlers[proc_type][impl]
    return handler(process,parameters)

def build_env(environment,parameters,state,pack_config):
    '''
    builds an environment template description and builds a fully-defined env
    this will use a handler in the future (just as build_job)
    '''

    env = copy.deepcopy(environment)
    if environment['environment_type'] == 'docker-encapsulated':
        for i,x in enumerate(env['par_mounts']):
            script = x.pop('jqscript')
            x['mountcontent'] = jq.jq(script).transform(parameters, text_output = True)

        if env['workdir'] is not None:
            env['workdir'] = state.contextualize_data(env['workdir'])
    return env

def run_in_env(environment,job,state,metadata,pack_config):
    '''
    takes a job and an environment and executes with the state context attached
    '''
    env_type = environment['environment_type']
    impl = pack_config.get_impl('environment',env_type)
    from .handlers.execution_handlers import handlers as exec_handlers
    handler = exec_handlers[env_type][impl]
    return handler(environment,state,job,metadata)

def publish(publisher,parameters,state, pack_config):
    pub_type   = publisher['publisher_type']
    impl = pack_config.get_impl('publisher',pub_type)
    from .handlers.publisher_handlers import handlers as pub_handlers
    handler = pub_handlers[pub_type][impl]
    return handler(publisher,parameters,state)

def prepublish(spec, parameters, state, pack_config):
    '''
    attempts to prepublish output data, returns None if not possible
    '''
    pub = spec['publisher']
    if pub['publisher_type'] in ['frompar-pub','constant-pub']:
        return publish(pub,parameters,state,pack_config)
    if pub['publisher_type'] == 'interpolated-pub':
        if pub['glob'] == False:
            return publish(pub,parameters,state,pack_config)
    return None

def run_packtivity(spec, parameters,state,metadata,config):
    with logutils.setup_logging_topic(metadata,state,'step',return_logger = True) as log:
        try:
            job = build_job(spec['process'], parameters, state, config)
            env = build_env(spec['environment'], parameters, state, config)
            run_in_env(env,job,state,metadata,config)
            pubdata = publish(spec['publisher'], parameters,state, config)
            log.info('publishing data: %s',pubdata)
            return pubdata
        except:
            log.exception('%s raised exception',metadata)
            raise

class defaultsyncbackend(object):
    def __init__(self,packconfig_spec = None):
        self.config = packconfig(**packconfig_spec) if packconfig_spec else packconfig()

    def prepublish(self,spec, parameters, state):
        return prepublish(spec, parameters, state, self.config)

    def run(self,spec,parameters,state, metadata = {'name': 'packtivity_syncbackend'}):
        return run_packtivity(spec,parameters,state,metadata,self.config)
