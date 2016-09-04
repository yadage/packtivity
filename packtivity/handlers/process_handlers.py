import packtivity.utils as utils
import click
import yaml
handlers,process = utils.handler_decorator()

@process('string-interpolated-cmd')
def stringinterp_handler(process_spec,attributes):
    flattened = {k:v if not (type(v)==list) else ' '.join([str(x) for x in v]) for k,v in attributes.iteritems()}
    return {
        'command':process_spec['cmd'].format(**flattened)
    }

@process('manual-instructions-proc')
def manual_proc(process_spec,attributes):
    instructions = process_spec['instructions']
    attrs = yaml.safe_dump(attributes,default_flow_style = False)
    click.secho(instructions, fg = 'blue')
    click.secho(attrs, fg = 'cyan')
