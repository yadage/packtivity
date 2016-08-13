import os
import errno

def handler_decorator():
    handlers = {}
    def decorator(name):
        def wrap(func):
            handlers[name] = func
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
