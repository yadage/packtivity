import yaml
import packtivity.utils as utils
import glob

handlers, publisher = utils.handler_decorator()

@publisher('frompar-pub')
def process_attr_pub_handler(publisher,attributes,context):
    outputs = {}
    for k,v in publisher['outputmap'].iteritems():
        outputs[k] = attributes[v]
    return outputs

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
