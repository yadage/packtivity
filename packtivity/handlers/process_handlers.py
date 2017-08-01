import click
import yaml

import packtivity.utils as utils
handlers,process = utils.handler_decorator()

@process('string-interpolated-cmd')
def stringinterp_handler(process_spec,parameters):
    flattened = {k:v if not (type(v)==list) else ' '.join([str(x) for x in v]) for k,v in parameters.items()}
    return {
        'command':process_spec['cmd'].format(**flattened)
    }

@process('interpolated-script-cmd')
def interp_script(process_spec,parameters):
    flattened = {k:v if not (type(v)==list) else ' '.join([str(x) for x in v]) for k,v in parameters.items()}
    return {
        'script':process_spec['script'].format(**flattened),
        'interpreter':process_spec['interpreter']
    }

@process('manual-instructions-proc')
def manual_proc(process_spec,parameters):
    instructions = process_spec['instructions']
    attrs = yaml.safe_dump(parameters,default_flow_style = False)
    click.secho(instructions, fg = 'blue')
    click.secho(attrs, fg = 'cyan')


@process('test-process')
def test_process(process_spec,parameters):
    return {
        'a': 'complicated',
        'job': 'with',
        'pars': parameters
    }
