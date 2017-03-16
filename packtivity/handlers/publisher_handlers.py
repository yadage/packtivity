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
def process_attr_pub_handler(publisher,attributes,context):
    outputs = copy.deepcopy(publisher['outputmap'])
    for path,value in utils.leaf_iterator(publisher['outputmap']):
        actualval = attributes[value]
        path.set(outputs,actualval)
    return outputs

@publisher('interpolated-pub')
def interpolated_pub_handler(publisher,attributes,context):
    forinterp = attributes.copy()
    forinterp.update(workdir = context['readwrite'][0])
    result = copy.deepcopy(publisher['publish'])
    for path,value in utils.leaf_iterator(publisher['publish']):
        path.set(result,value.format(**forinterp))
    return result

@publisher('fromyaml-pub')
def fromyaml_pub_handler(publisher,attributes,context):
    workdir  = context['readwrite'][0]
    yamlfile =  publisher['yamlfile']
    pubdata = yaml.load(open('{}/{}'.format(workdir,yamlfile)))
    return pubdata

@publisher('fromglob-pub')
def glob_pub_handler(publisher,attributes,context):
    workdir = context['readwrite'][0]
    globexpr =  publisher['globexpression']
    return {publisher['outputkey']:glob2.glob('{}/{}'.format(workdir,globexpr))}

@publisher('constant-pub')
def dummy_pub_handler(publisher,attributes,context):
    return  publisher['publish']

@publisher('manual-publishing')
def manual_pub(publisher,attributes,context):
    instructions = publisher['instructions']
    click.secho(instructions, fg = 'magenta')
    while True:
        published_json = raw_input("Enter JSON data to publish: ")
        try:
            data = json.loads(published_json)
        except:
            click.secho('uhm something went wrong, enter valid JSON please', fg = 'red')
            continue
        shall = raw_input("got: \n {} \npublish? (y/N) ".format(yaml.safe_dump(data,default_flow_style = False))).lower() == 'y'
        if shall:
            break
    click.secho('publishing', fg = 'green')
    return data

@publisher('gridoutput-pub')
def gridoutput_pub(publisher,attributes,context):
    import grid_handlers
    return grid_handlers.publish_grid_job(publisher,attributes,context)
