import click
import packtivity
import os
import jsonschema
import yaml
from packtivity.loader import load_and_validate

@click.command()
@click.argument('spec')
@click.argument('context')
@click.argument('kwargs')
@click.option('-n','--name', default = 'pack')
@click.option('-w','--workdir', default = os.getcwd())
@click.option('-s','--source', default = None)
def runcli(spec,context,kwargs,name,workdir,source):
    spec   = load_and_validate(spec,source,'step-schema')
    ctx    = yaml.load(open(context))
    ctx.update(workdir = workdir)
    kwargs = yaml.load(open(kwargs))
    result = packtivity.packtivity(name,spec,kwargs,ctx)
    click.echo(result)

@click.command()
@click.argument('spec')
@click.option('-s','--source', default = None)
def validatecli(spec,source):
    try:
        spec   = load_and_validate(spec,source,'step-schema')
    except jsonschema.exceptions.ValidationError:
        raise click.ClickException(click.style('not valid',fg = 'red'))
    click.secho('valid',fg = 'green')
    
