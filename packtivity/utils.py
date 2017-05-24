import os
import errno
import jq
import jsonpointer

def handler_decorator():
    handlers = {}
    def decorator(name, implementation = 'default'):
        def wrap(func):
            handlers.setdefault(name,{})[implementation] = func
        return wrap
    return handlers,decorator

def mkdir_p(path):
    #http://stackoverflow.com/questions/600268/mkdir-p-functionality-in-python
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def leaf_iterator(jsonable):
    allleafs = jq.jq('leaf_paths').transform(jsonable, multiple_output = True)
    leafpointers = [jsonpointer.JsonPointer.from_parts(x) for x in allleafs]
    for x in leafpointers:
        yield x,x.get(jsonable)
