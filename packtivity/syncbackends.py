import yaml
import os

import packtivity.logutils as logutils
from packtivity.typedleafs import TypedLeafs

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
    return handler(process,parameters, state)

def build_env(environment,parameters,state,pack_config):
    '''
    builds an environment template description and builds a fully-defined env
    this will use a handler in the future (just as build_job)
    '''

    env_type =  environment['environment_type']
    impl = pack_config.get_impl('environment',env_type)
    from .handlers.environment_handlers import handlers as env_handlers
    try:
        handler = env_handlers[env_type][impl]
    except KeyError:
        handler = env_handlers['default']['default']
    return handler(environment,parameters,state)

def run_in_env(job,environment,state,metadata,pack_config):
    '''
    takes a job and an environment and executes with the state context attached
    '''
    env_type = environment['environment_type']
    impl = pack_config.get_impl('executor',env_type)
    from .handlers.execution_handlers import handlers as exec_handlers
    handler = exec_handlers[env_type][impl]
    return handler(environment,state,job,metadata)

def publish(publisher,parameters,state, pack_config):
    pub_type   = publisher['publisher_type']
    impl = pack_config.get_impl('publisher',pub_type)
    from .handlers.publisher_handlers import handlers as pub_handlers
    handler = pub_handlers[pub_type][impl]
    pubdata = handler(publisher,parameters,state)
    return TypedLeafs(pubdata, state.datamodel if state else None)

def finalize_inputs(parameters, state):
    parameters = TypedLeafs(parameters,state.datamodel if state else None)
    if not state: return parameters, state
    return state.model(parameters), state

def finalize_outputs(pubdata):
    return pubdata

def prepublish(spec, parameters, state, pack_config):
    '''
    attempts to prepublish output data, returns None if not possible
    '''
    parameters, state = finalize_inputs(parameters, state)
    pub = spec['publisher']

    pubdata = None
    if pub['publisher_type'] in ['frompar-pub','constant-pub']:
        return publish(pub,parameters,state,pack_config)
    if pub['publisher_type'] in ['interpolated-pub', 'fromparjq-pub']:
        from .statecontexts.posixfs_context import LocalFSState
        if not state:
            return publish(pub,parameters,state,pack_config)
        if type(state) == LocalFSState:
            if pub['glob'] == False or len(state.readwrite)==0:
                pubdata = publish(pub,parameters,state,pack_config)
    return pubdata

def acquire_job_env(spec, parameters,state,metadata,config):
    if spec['process'] and spec['environment']:
        job = build_job(spec['process'], parameters, state, config)
        env = build_env(spec['environment'], parameters, state, config)
        return job, env
    return None, None

def run_packtivity(spec, parameters,state,metadata,config):
    with logutils.setup_logging_topic(metadata,state,'step',return_logger = True) as log:
        parameters, state = finalize_inputs(parameters, state)
        job, env = acquire_job_env(spec, parameters,state,metadata,config)

        if job and env:
            try:
                run_in_env(job, env,state,metadata,config)
            except:
                log.exception('job execution if job %s raise exception exception',metadata)
                raise

        pubdata = publish(spec['publisher'], parameters,state, config)
        pubdata = finalize_outputs(pubdata)
        log.info('publishing data: %s',pubdata)
        return pubdata

class defaultsyncbackend(object):
    def __init__(self,packconfig_spec = None):
        self.config = packconfig(**packconfig_spec) if packconfig_spec else packconfig()

    def prepublish(self,spec, parameters, state):
        return prepublish(spec, parameters, state, self.config)

    def run(self,spec,parameters,state, metadata = {'name': 'packtivity_syncbackend'}):
        return run_packtivity(spec,parameters,state,metadata,self.config)
