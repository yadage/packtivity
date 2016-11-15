import yaml
import packtivity.utils as utils
import glob
import json
import click

handlers, publisher = utils.handler_decorator()

@publisher('frompar-pub')
def process_attr_pub_handler(publisher,attributes,context):
    outputs = {}
    for k,v in publisher['outputmap'].iteritems():
        outputs[k] = attributes[v]
    return outputs

@publisher('interpolated-pub')
def interpolated_pub_handler(publisher,attributes,context):
    forinterp = attributes.copy()
    forinterp.update(workdir = context['readwrite'][0])
    result = publisher['publish'].copy()
    for k,v in  result.iteritems():
        result[k] = v.format(**forinterp)
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
    return {publisher['outputkey']:glob.glob('{}/{}'.format(workdir,globexpr))}

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
