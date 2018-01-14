import packtivity.utils as utils

handlers,environment = utils.handler_decorator()

@environment('docker-encapsulated')
def docker(environment,parameters,state):
    for i,x in enumerate(environment['par_mounts']):
        script = x.pop('jqscript')
        x['mountcontent'] = jq.jq(script).transform(parameters, text_output = True)

    if environment['workdir'] is not None:
        environment['workdir'] = state.contextualize_value(environment['workdir'])
    return environment


@environment('default')
def default(environment,parameters,state):
    return environment
