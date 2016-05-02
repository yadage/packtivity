import utils

handlers,process = utils.handler_decorator()

@process('string-interpolated-cmd')
def stringinterp_handler(process_spec,attributes):
    flattened = {k:v if not (type(v)==list) else ' '.join([str(x) for x in v]) for k,v in attributes.iteritems()}
    return {
        'command':process_spec['cmd'].format(**flattened)
    }