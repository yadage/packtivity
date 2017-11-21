import yaml
import glob2
import json
import click
import copy
import logging
import jq
import os
from six import string_types


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
        if not isinstance(value, string_types): continue
        resultval = value.format(**forinterp)
        globexpr = resultval
        if publisher['relative_paths'] and os.path.commonprefix([state.readwrite[0],globexpr]) == '':
            globexpr = os.path.join(state.readwrite[0],resultval)
        if publisher['glob']:
            globbed = glob2.glob(globexpr)
            if globbed:
                resultval = globbed
        else:
             #if it's a string and the full path exists replace relative path
             resultval = globexpr
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
        if not isinstance(value, string_types): continue
        if not state: break
        if state.readwrite:
            globdir = state.readwrite[0]
        elif state.readonly and len(state.readonly)==1:
            globdir = state.readonly[0]
        else: break
        globexpr = value
        if publisher['relative_paths'] and os.path.commonprefix([globdir,globexpr]) == '':
            globexpr = os.path.join(globdir,value)
        if publisher['glob']:
            globbed = glob2.glob(globexpr)
            if globbed:
                value = globbed
        else:
             #if it's a string and the full path exists replace relative path
             value = globexpr
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
