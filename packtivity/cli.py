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


def getinit_data(initfiles, parameters):
    """
    get initial data from both a list of files and a list of 'pname=pvalue'
    strings as they are passed in the command line <pvalue> is assumed to be a
    YAML parsable string.
    """
    initdata = {}
    for initfile in initfiles:
        initdata.update(**yaml.safe_load(open(initfile)))

    for x in parameters:
        key, value = x.split("=")
        initdata[key] = yaml.safe_load(value)
    return initdata


@click.command()
@click.option("--parameter", "-p", multiple=True)
@click.option("-r", "--read", multiple=True, default=[])
@click.option("-w", "--write", multiple=True, default=[os.curdir])
@click.option("-s", "--state", default="")
@click.option("-t", "--toplevel", default=os.getcwd())
@click.option("-c", "--schemasource", default=yadageschemas.schemadir)
@click.option("-v", "--verbosity", default="ERROR")
@click.option("--validate/--no-validate", default=True)
@click.option("--asyncwait/--async", default=True)
@click.option("-b", "--backend", default="defaultsync")
@click.option("-x", "--proxyfile", default="proxy.json")
@click.option("-o", "--outfile", default=None)
@click.argument("spec")
@click.argument("parfiles", nargs=-1)
def runcli(
    spec,
    parfiles,
    state,
    parameter,
    read,
    write,
    toplevel,
    schemasource,
    asyncwait,
    validate,
    verbosity,
    backend,
    proxyfile,
    outfile,
):
    logging.basicConfig(level=getattr(logging, verbosity))

    spec = utils.load_packtivity(spec, toplevel, schemasource, validate)

    parameters = getinit_data(parfiles, parameter)

    state = yaml.safe_load(open(state)) if state else {}
    if not state:
        state.setdefault("readwrite", []).extend(list(map(os.path.realpath, write)))
        state.setdefault("readonly", []).extend(list(map(os.path.realpath, read)))
    state = LocalFSState(state["readwrite"], state["readonly"])
    state.ensure()

    is_sync, backend = bkutils.backend_from_string(backend)
    backend_kwargs = (
        {"syncbackend": backend}
        if is_sync
        else {"asyncbackend": backend, "asyncwait": asyncwait}
    )

    prepub = backend.prepublish(spec, parameters, state)
    if prepub:
        click.echo(str(prepub) + (" (prepublished)"))

    pack = packtivity.pack_object(spec)

    result = pack(parameters, state, **backend_kwargs)

    if not is_sync and not asyncwait:
        click.secho("proxy-json {}".format(json.dumps(result.json())))
        with open(proxyfile, "w") as p:
            json.dump(result.json(), p)
    else:
        click.echo(str(result) + (" (post-run)" if prepub else ""))
        if outfile:
            with open(outfile, "w") as out:
                out.write(json.dumps(result.json()))


@click.command()
@click.argument("spec")
@click.option("-t", "--toplevel", default=os.getcwd())
@click.option("-c", "--schemasource", default=yadageschemas.schemadir)
@click.option("-n", "--schemaname", default="packtivity/packtivity-schema")
@click.option("--show/--no-show", default=False)
def validatecli(spec, toplevel, schemasource, schemaname, show):
    try:
        spec = utils.load_packtivity(spec, toplevel, schemasource, validate=True)
        if show:
            click.echo(json.dumps(dict(spec)))
        else:
            click.secho("packtivity definition is valid", fg="green")
    except jsonschema.exceptions.ValidationError as e:
        click.echo(e)
        raise click.ClickException(
            click.style("packtivity definition not valid", fg="red")
        )


@click.group()
def utilcli():
    pass


@utilcli.command()
@click.option("-r", "--read", multiple=True, default=[])
@click.option("-w", "--write", multiple=True, default=[os.curdir])
@click.option("-s", "--state", default="")
@click.option("--parameter", "-p", multiple=True)
@click.option("-t", "--toplevel", default=os.getcwd())
@click.option("-c", "--schemasource", default=yadageschemas.schemadir)
@click.option("-v", "--verbosity", default="ERROR")
@click.option("--validate/--no-validate", default=True)
@click.option("-b", "--backend", default="defaultsync")
@click.argument("spec")
@click.argument("parfiles", nargs=-1)
def pubtest(
    spec,
    parfiles,
    state,
    parameter,
    read,
    write,
    toplevel,
    schemasource,
    validate,
    verbosity,
    backend,
):
    logging.basicConfig(level=getattr(logging, verbosity))
    spec = utils.load_packtivity(spec, toplevel, schemasource, validate)
    state = yaml.safe_load(open(state)) if state else {}
    if not state:
        state.setdefault("readwrite", []).extend(list(map(os.path.realpath, write)))
        state.setdefault("readonly", []).extend(list(map(os.path.realpath, read)))
    state = LocalFSState(state["readwrite"], state["readonly"])

    parameters = getinit_data(parfiles, parameter)

    is_sync, backend = bkutils.backend_from_string(backend)
    publish = backend.prepublish(spec, parameters, state)
    click.echo(str(publish))


@utilcli.command()
@click.option("-r", "--read", multiple=True, default=[])
@click.option("-w", "--write", multiple=True, default=[os.curdir])
@click.option("-s", "--state", default="")
@click.option("--parameter", "-p", multiple=True)
@click.option("-t", "--toplevel", default=os.getcwd())
@click.option("-c", "--schemasource", default=yadageschemas.schemadir)
@click.option("-v", "--verbosity", default="ERROR")
@click.option("--validate/--no-validate", default=True)
@click.option("-b", "--backend", default="defaultsync")
@click.argument("spec")
@click.argument("parfiles", nargs=-1)
def shell(
    spec,
    parfiles,
    state,
    parameter,
    read,
    write,
    toplevel,
    schemasource,
    validate,
    verbosity,
    backend,
):
    logging.basicConfig(level=getattr(logging, verbosity))

    from .datamodel import create

    parameters = create(getinit_data(parfiles, parameter))

    spec = utils.load_packtivity(spec, toplevel, schemasource, validate)
    state = yaml.safe_load(open(state)) if state else {}
    if not state:
        state.setdefault("readwrite", []).extend(list(map(os.path.realpath, write)))
        state.setdefault("readonly", []).extend(list(map(os.path.realpath, read)))
    state = LocalFSState(state["readwrite"], state["readonly"])

    state.ensure()

    _, backend = packtivity.backendutils.backend_from_string("foregroundasync")

    env = packtivity.syncbackends.build_env(
        spec["environment"], parameters, state, backend.pack_config
    )

    if spec["process"]["process_type"] == "interpolated-script-cmd":
        job = {"interactive": spec["process"]["interpreter"]}
    else:
        job = {"interactive": "sh"}
    metadata = {"name": "test"}

    result = packtivity.syncbackends.run_in_env(
        job, env, state, metadata, backend.pack_config, backend.exec_config
    )

    print(result)


@click.command()
@click.argument("jsonfile")
def checkproxy(jsonfile):
    proxydata = json.load(open(jsonfile))
    proxy, backend = bkutils.load_proxy(proxydata, best_effort_backend=True)

    ready = backend.ready(proxy)

    click.secho("ready: {}".format(ready))
    if ready:
        successful = backend.successful(proxy)
        click.secho("successful: {}".format(successful))
        if successful:
            result = backend.result(proxy)
            click.secho("result: {}".format(json.dumps(result.json())))
