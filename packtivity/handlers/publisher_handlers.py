import yaml
import packtivity.utils as utils
import glob2
import json
import click
import copy
import logging

log = logging.getLogger(__name__)
handlers, publisher = utils.handler_decorator()

@publisher('frompar-pub')
def process_attr_pub_handler(publisher,attributes,state):
    outputs = copy.deepcopy(publisher['outputmap'])
    for path,value in utils.leaf_iterator(publisher['outputmap']):
        actualval = attributes[value]
        path.set(outputs,actualval)
    return outputs

@publisher('interpolated-pub')
def interpolated_pub_handler(publisher,attributes,state):
    forinterp = attributes.copy()
    forinterp.update(workdir = state.readwrite[0])
    result = copy.deepcopy(publisher['publish'])
    for path,value in utils.leaf_iterator(publisher['publish']):
        resultval = value.format(**forinterp)
        if publisher['glob']:
            resultval = glob2.glob(resultval)
        path.set(result,resultval)
    return result

@publisher('fromyaml-pub')
def fromyaml_pub_handler(publisher,attributes,state):
    workdir  = state.readwrite[0]
    yamlfile =  publisher['yamlfile']
    pubdata = yaml.load(open('{}/{}'.format(workdir,yamlfile)))
    return pubdata

@publisher('fromglob-pub')
def glob_pub_handler(publisher,attributes,state):
    workdir = state.readwrite[0]
    globexpr =  publisher['globexpression']
    return {publisher['outputkey']:glob2.glob('{}/{}'.format(workdir,globexpr))}

@publisher('constant-pub')
def dummy_pub_handler(publisher,attributes,state):
    return  publisher['publish']

@publisher('manual-publishing')
def manual_pub(publisher,attributes,state):
    instructions = publisher['instructions']
    click.secho(instructions, fg = 'magenta')
    while True:
        published_json = raw_input("Enter JSON data to publish: ")
        try:
            data = json.loads(published_json)
        except:
            click.secho('uhm something went wrong, enter valid JSON please', fg = 'red')
            continue
        shall = raw_input("got: \n {} \npublish? (y/N) ".format(yaml.safe_dump(data, default_flow_style = False))).lower() == 'y'
        if shall:
            break
    click.secho('publishing', fg = 'green')
    return data

