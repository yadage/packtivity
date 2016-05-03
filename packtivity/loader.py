import capschemas

def load(source, toplevel, schema_name = schema_name, schemadir = None, validate = True):
    data = capschemas.load(source,toplevel,'packtivity/packtivity-schema',schemadir,validate)
    return data