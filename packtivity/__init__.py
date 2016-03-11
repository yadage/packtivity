import os
import logging

import pkg_resources
schemadir = pkg_resources.resource_filename('packtivity','schema')


def publish(step,context):
    pubtype =  step['step_spec']['publisher']['publisher-type']
    from handlers.publisher_handlers import handlers as pub_handlers
    publisher = pub_handlers[pubtype]
    return publisher(step,context)

def build_command(process,attributes):
    proc_type =  process['process-type']
    from handlers.process_handlers import handlers as proc_handlers
    handler = proc_handlers[proc_type]
    command = handler(process,attributes)
    return command

def run_in_env(environment,cmd,context,log,nametag):
    from handlers.environment_handlers import handlers as env_handlers
    handlercls = env_handlers[environment['environment-type']]
    handler = handlercls(nametag,log)
    return handler(environment,context,cmd)

def runstep(step,global_context):
    steplog = '{}/{}.step.log'.format(os.path.abspath(global_context['workdir']),step['name'])
    log = logging.getLogger('step_logger_{}'.format(step['name']))

    fh  = logging.FileHandler(steplog)
    fh.setLevel(logging.DEBUG)
    log.addHandler(fh)

    log.debug('starting log for step: %s',step['name'])

    command = build_command(step['step_spec']['process'],step['attributes'])

    environment = step['step_spec']['environment']
    run_in_env(environment,command,global_context,log,step['name'])
    output      = publish(step,global_context)
    return output

class packtivity(object):
    def __init__(self,name,spec,context):
        self.step_info = {}
        self.step_info['name'] = name
        self.step_info['step_spec'] = spec
        self.step_info['attributes'] = {}
        self.context = context

    def __repr__(self):
        return '<packtivity name: {}>'.format(self.name)

    @property
    def step(self):
        return self.step_info
    
    @property
    def name(self):
        return self.step_info['name']
    
    def attr(self,key,value):
        self.step_info['attributes'][key] = value
    def s(self,**attributes):
        self.step_info['attributes'] = attributes
        return self
        
    def __call__(self,**attributes):
        self.step_info['attributes'].update(**attributes)
        return runstep(self.step_info,global_context = self.context)