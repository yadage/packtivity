import click
import packtivity
import os
import jsonschema
import yaml
from packtivity.loader import load_and_validate

@click.command()
@click.argument('spec')
@click.argument('parameters', default = '')
@click.option('-c','--context', default = None)
@click.option('-n','--name', default = 'pack')
@click.option('-w','--workdir', default = os.getcwd())
@click.option('-s','--source', default = None)
def runcli(spec,parameters,context,name,workdir,source):
    spec   = load_and_validate(spec,source,'step-schema')
    ctx    = yaml.load(open(context)) if context else {}
    ctx.update(workdir = workdir)
    parameters = yaml.load(open(parameters)) if parameters else {}
    result = packtivity.packtivity(name,spec,parameters,ctx)
    click.echo(result)

@click.command()
@click.argument('spec')
@click.option('-s','--source', default = os.getcwd())
@click.option('-c','--schemasource', default = os.getcwd())
@click.option('-n','--schemaname', default = 'step-schema')
def validatecli(spec,source,schemasource,schemaname):
    try:
        spec   = load_and_validate(spec,source,schemaname, schemadir = schemasource)
    except jsonschema.exceptions.ValidationError as e:
        click.echo(e)
        raise click.ClickException(click.style('not valid',fg = 'red'))
    click.secho('valid',fg = 'green')
    
