import yaml
import utils

handlers, publisher = utils.handler_decorator()

@publisher('process-attr-pub')
def process_attr_pub_handler(publisher,attributes,context):
    outputs = {}
    for k,v in publisher['outputmap'].iteritems():
      outputs[k] = [attributes[v]]
    return outputs
    
@publisher('fromyaml-pub')
def fromyaml_pub_handler(publisher,attributes,context):
    yamlfile =  publisher['yamlfile']
    yamlfile =  yamlfile.replace('/workdir',context['workdir'])
    pubdata = yaml.load(open(yamlfile))
    return pubdata
    
@publisher('dummy-pub')
def dummy_pub_handler(publisher,attributes,context):
    return  publish['publish']
