import click
import packtivity
import os
import jsonschema
import yaml
import yadageschemas
import logging
import json

import packtivity.utils as utils
import packtivity.backendutils as bkutils
from .statecontexts.posixfs_context import LocalFSState

log = logging.getLogger(__name__)

def finalize_input(jsondata,state):
    for path,value in utils.leaf_iterator(jsondata):
        actualval = state.contextualize_data(value)
        path.set(jsondata,actualval)
    return jsondata

def getinit_data(initfiles,parameters):
    '''
    get initial data from both a list of files and a list of 'pname=pvalue'
    strings as they are passed in the command line <pvalue> is assumed to be a
    YAML parsable string.
    '''
    initdata = {}
    for initfile in initfiles:
        initdata.update(**yaml.load(open(initfile)))

    for x in parameters:
        key,value = x.split('=')
        initdata[key]=yaml.load(value)
    return initdata

@click.command()
@click.option('--parameter', '-p', multiple=True)
@click.option('-r', '--read', multiple=True, default = [])
@click.option('-w', '--write', multiple=True, default = [os.curdir])
@click.option('--contextualize/--no-contextualize', default = True)
@click.option('-s','--state', default = '')
@click.option('-t','--toplevel', default = os.getcwd())
@click.option('-c','--schemasource', default = yadageschemas.schemadir)
@click.option('-v','--verbosity', default = 'ERROR')
@click.option('--validate/--no-validate', default = True)
@click.option('--asyncwait/--async', default = True)
@click.option('-b','--backend',default = 'defaultsync')
@click.option('-x','--proxyfile',default = 'proxy.json')
@click.argument('spec')
@click.argument('parfiles', nargs = -1)
def runcli(spec,parfiles,state,parameter,read,write,toplevel,schemasource,asyncwait,contextualize,validate,verbosity,backend,proxyfile):
    logging.basicConfig(level = getattr(logging,verbosity))

    spec = utils.load_packtivity(spec,toplevel,schemasource,validate)

    parameters = getinit_data(parfiles,parameter)

    state    = yaml.load(open(state)) if state else {}
    state.setdefault('readwrite',[]).extend(map(os.path.realpath,write))
    state.setdefault('readonly',[]).extend(map(os.path.realpath,read))
    state = LocalFSState(state['readwrite'],state['readonly'])
    state.ensure()

    if contextualize:
        parameters = finalize_input(parameters,state)

    is_sync, backend = bkutils.backend_from_string(backend)
    backend_kwargs = {
        'syncbackend': backend
    } if is_sync else {
        'asyncbackend':backend,
        'asyncwait': asyncwait
    }

    prepub = backend.prepublish(spec,parameters,state)
    if prepub:
        click.echo(str(prepub)+(' (prepublished)'))

    pack = packtivity.pack_object(spec)

    result = pack(parameters,state,**backend_kwargs)

    if not is_sync and not asyncwait:
        click.secho('proxy-json {}'.format(json.dumps(result.json())))
        with open(proxyfile,'w') as p:
            json.dump(result.json(),p)
    else:
        click.echo(str(result)+(' (post-run)' if prepub else ''))

@click.command()
@click.argument('spec')
@click.option('-t','--toplevel', default = os.getcwd())
@click.option('-c','--schemasource', default = yadageschemas.schemadir)
@click.option('-n','--schemaname', default = 'packtivity/packtivity-schema')
@click.option('--show/--no-show', default = False)
def validatecli(spec,toplevel,schemasource,schemaname,show):
    try:
        spec = utils.load_packtivity(spec,toplevel,schemasource,validate = True)
        if show:
            click.echo(json.dumps(dict(spec)))
        else:
            click.secho('packtivity definition is valid',fg = 'green')
    except jsonschema.exceptions.ValidationError as e:
        click.echo(e)
        raise click.ClickException(click.style('packtivity definition not valid',fg = 'red'))

@click.command()
@click.argument('jsonfile')
def checkproxy(jsonfile):
    proxydata = json.load(open(jsonfile))
    proxy, backend = bkutils.load_proxy(proxydata, best_effort_backend = True)

    ready = backend.ready(proxy)

    click.secho('ready: {}'.format(ready))
    if ready:
        successful = backend.successful(proxy)
        click.secho('successful: {}'.format(successful))
        if successful:
            result = backend.result(proxy)
            click.secho('result: {}'.format(json.dumps(result)))
