import click
import packtivity
import os
import jsonschema
import yaml
import capschemas
import logging
log = logging.basicConfig(level = logging.INFO)

#stolen from yadage
def finalize_value(value,context):
    if type(value)==list:
        return [finalize_value(x,context) for x in value]
    if type(value) in [str,unicode]:
        return value.format(**context)
    return value

def finalize_input(json,context):
    context['workdir'] = context['readwrite'][0]
    result = {}
    for k,v in json.iteritems():
        if type(v) is not list:
            result[k] = finalize_value(v,context)
        else:
            result[k] = [finalize_value(element,context) for element in v]
    return result

@click.command()
@click.argument('spec')
@click.argument('parameters', default = '')
@click.option('-c','--context', default = None)
@click.option('-n','--name', default = 'pack')
@click.option('-w','--workdir', default = os.getcwd())
@click.option('-s','--source', default = os.getcwd())
@click.option('-o','--schemasource', default = capschemas.schemadir)
def runcli(spec,parameters,context,name,workdir,source,schemasource):
    spec   = capschemas.load(spec,source,'packtivity/packtivity-schema',schemadir = schemasource)
    parameters = yaml.load(open(parameters)) if parameters else {}


    ctx    = yaml.load(open(context)) if context else {}
    if 'readwrite' not in ctx:
        ctx['readwrite'] = [workdir]
    else:
        ctx['readwrite'] += [workdir]
    if 'readonly' not in ctx:
        ctx['readonly'] = []

    #interpolate parameters out of courtesy
    parameters = finalize_input(parameters,ctx)

    p = packtivity.packtivity_callable(name,spec,parameters,ctx)
    prepub = p.published_data is not None
    if p.published_data:
        click.echo(str(p.published_data)+(' (prepublished)' if prepub else ''))

    result = p()
    if not p.published_data:
        click.echo(result)

@click.command()
@click.argument('spec')
@click.option('-s','--source', default = os.getcwd())
@click.option('-c','--schemasource', default = capschemas.schemadir)
@click.option('-n','--schemaname', default = 'packtivity/packtivity-schema')
def validatecli(spec,source,schemasource,schemaname):
    try:
        spec   = capschemas.load(spec,source,schemaname, schemadir = schemasource)
    except jsonschema.exceptions.ValidationError as e:
        click.echo(e)
        raise click.ClickException(click.style('not valid',fg = 'red'))
    click.secho('valid',fg = 'green')
