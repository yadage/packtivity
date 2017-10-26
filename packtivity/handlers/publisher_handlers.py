import yaml
import glob2
import json
import click
import copy
import logging
import jq
import os

import packtivity.utils as utils

log = logging.getLogger(__name__)
handlers, publisher = utils.handler_decorator()

@publisher('frompar-pub')
def process_attr_pub_handler(publisher,parameters,state):
    outputs = copy.deepcopy(publisher['outputmap'])
    for path,value in utils.leaf_iterator(publisher['outputmap']):
        actualval = parameters[value]
        path.set(outputs,actualval)
    return outputs

@publisher('interpolated-pub')
def interpolated_pub_handler(publisher,parameters,state):
    forinterp = parameters.copy()
    forinterp.update(workdir = state.readwrite[0])
    result = copy.deepcopy(publisher['publish'])
    for path,value in utils.leaf_iterator(publisher['publish']):
        resultval = value.format(**forinterp)
        if publisher['relative_paths'] and os.path.commonprefix([state.readwrite[0],resultval]) == '':
            resultval = os.path.join(state.readwrite[0],resultval)
        if publisher['glob']:
            resultval = glob2.glob(resultval)
        path.set(result,resultval)
    return result

@publisher('fromyaml-pub')
def fromyaml_pub_handler(publisher,parameters,state):
    workdir  = state.readwrite[0]
    yamlfile =  publisher['yamlfile']
    pubdata = yaml.load(open('{}/{}'.format(workdir,yamlfile)))

    return pubdata

@publisher('fromparjq-pub')
def fromparjq_pub(publisher,parameters,state):
    inputjson = parameters
    result = jq.jq(publisher['script']).transform(inputjson)
    assert type(result)==dict
    for path,value in utils.leaf_iterator(result):
        if publisher['relative_paths'] and os.path.commonprefix([state.readwrite[0],value]) == '':
            value = os.path.join(state.readwrite[0],value)
        if publisher['glob']:
            value = glob2.glob(value)
        path.set(result,value)
    return result

@publisher('fromglob-pub')
def glob_pub_handler(publisher,parameters,state):
    workdir = state.readwrite[0]
    globexpr =  publisher['globexpression']
    return {publisher['outputkey']:glob2.glob('{}/{}'.format(workdir,globexpr))}

@publisher('constant-pub')
def dummy_pub_handler(publisher,parameters,state):
    return  publisher['publish']

@publisher('manual-publishing')
def manual_pub(publisher,parameters,state):
    instructions = publisher['instructions']
    click.secho(instructions, fg = 'magenta')
    while True:
        try:
            published_json = raw_input("Enter JSON data to publish: ")
        except NameError:
            published_json = input("Enter JSON data to publish: ")
        try:
            data = json.loads(published_json)
        except:
            click.secho('uhm something went wrong, enter valid JSON please', fg = 'red')
            continue
        try:
            shall = raw_input("got: \n {} \npublish? (y/N) ".format(yaml.safe_dump(data, default_flow_style = False))).lower() == 'y'
        except NameError:
            shall = input("got: \n {} \npublish? (y/N) ".format(yaml.safe_dump(data, default_flow_style = False))).lower() == 'y'
        if shall:
            break
    click.secho('publishing', fg = 'green')
    return data
