import pkg_resources
schemadir = pkg_resources.resource_filename('packtivity','schema')
    
def publish(publisher,attributes,context):
    pub_type   = publisher['publisher_type']
    from handlers.publisher_handlers import handlers as pub_handlers
    handler = pub_handlers[pub_type]
    return handler(publisher,attributes,context)
    
def build_command(process,attributes):
    proc_type =  process['process_type']
    from handlers.process_handlers import handlers as proc_handlers
    handler = proc_handlers[proc_type]
    return handler(process,attributes)
    
def run_in_env(nametag,environment,command,context):
    env_type = environment['environment_type']
    from handlers.environment_handlers import handlers as env_handlers
    handler = env_handlers[env_type]
    return handler(nametag,environment,context,command)
    
def packtivity(uniquetag,step,attributes,context):
    command = build_command(step['process'],attributes)
    run_in_env(uniquetag,step['environment'],command,context)
    output  = publish(step['publisher'],attributes,context)
    return output