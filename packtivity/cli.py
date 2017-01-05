import click
import packtivity
import os
import jsonschema
import yaml
import yadageschemas
import logging
import json
import packtivity.utils as utils

log = logging.getLogger(__name__)

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

def load_pack(spec,toplevel,schemasource,validate):
    #in case that spec is a json reference string, we will treat it as such
    #if it's just a filename, this should not affect it...
    spec   = yadageschemas.load(
            {'$ref':spec},
            toplevel,
            'packtivity/packtivity-schema',
            schemadir = schemasource,
            validate = validate,
            initialload = False
    )
    return spec

@click.command()
@click.option('--parameter', '-p', multiple=True)
@click.option('-r', '--read', multiple=True, default = [])
@click.option('-w', '--write', multiple=True, default = [os.curdir])
@click.option('--contextualize/--no-contextualize', default = True)
@click.option('-c','--context', default = '')
@click.option('-t','--toplevel', default = os.getcwd())
@click.option('-s','--schemasource', default = yadageschemas.schemadir)
@click.option('-v','--verbosity', default = 'ERROR')
@click.option('--validate/--no-validate', default = True)
@click.option('--asyncwait/--async', default = True)
@click.option('-b','--backend',default = 'defaultsync')
@click.option('-x','--proxyfile',default = 'proxy.json')
@click.argument('spec')
@click.argument('parfiles', nargs = -1)
def runcli(spec,parfiles,context,parameter,read,write,toplevel,schemasource,asyncwait,contextualize,validate,verbosity,backend,proxyfile):
    logging.basicConfig(level = getattr(logging,verbosity))

    spec = load_pack(spec,toplevel,schemasource,validate)

    parameters = getinit_data(parfiles,parameter)

    context    = yaml.load(open(context)) if context else {}

    context.setdefault('readwrite',[]).extend(map(os.path.realpath,write))
    context.setdefault('readonly',[]).extend(map(os.path.realpath,read))

    if contextualize:
        parameters = finalize_input(parameters,context)

    is_sync, backend = utils.backend_from_string(backend)
    backend_kwargs = {
        'syncbackend': backend
    } if is_sync else {
        'asyncbackend':backend,
        'asyncwait': asyncwait
    }

    prepub = backend.prepublish(spec,parameters,context)
    if prepub:
        click.echo(str(prepub)+(' (prepublished)'))

    pack = packtivity.pack_object(spec)


    print 'calling pack with backend kwargs', backend_kwargs
    result = pack(parameters,context,**backend_kwargs)

    if not is_sync and not asyncwait:
        print 'this is a proxy'
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
def validatecli(spec,toplevel,schemasource,schemaname):
    try:
        spec = load_pack(spec,toplevel,schemasource,validate = True)
    except jsonschema.exceptions.ValidationError as e:
        click.echo(e)
        raise click.ClickException(click.style('packtivity definition not valid',fg = 'red'))
    click.secho('packtivity definition is valid',fg = 'green')

@click.command()
@click.argument('jsonfile')
def checkproxy(jsonfile):
    proxydata = json.load(open(jsonfile))
    proxy, backend = utils.proxy_from_json(proxydata, best_effort_backend = True)

    ready = backend.ready(proxy)

    click.secho('ready: {}'.format(ready))
    if ready:
        successful = backend.successful(proxy)
        click.secho('successful: {}'.format(successful))
        if successful:
            result = backend.result(proxy)
            click.secho('result: {}'.format(json.dumps(result)))
