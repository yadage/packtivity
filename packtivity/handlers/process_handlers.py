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

@process('interpolated-script-cmd')
def interp_script(process_spec,attributes):
    flattened = {k:v if not (type(v)==list) else ' '.join([str(x) for x in v]) for k,v in attributes.iteritems()}
    return {
        'script':process_spec['script'].format(**flattened),
        'interpreter':process_spec['interpreter']
    }

@process('manual-instructions-proc')
def manual_proc(process_spec,attributes):
    instructions = process_spec['instructions']
    attrs = yaml.safe_dump(attributes,default_flow_style = False)
    click.secho(instructions, fg = 'blue')
    click.secho(attrs, fg = 'cyan')

@process('grid-transform')
def grid_transform(process_spec,attributes):
    import grid_handlers
    return grid_handlers.build_grid_job(process_spec,attributes)
