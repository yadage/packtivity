import itertools
import os
import shutil
import packtivity.utils as utils

def make_new_context(name,oldcontext = None, subdir = True, create = False):
    '''
    creates a new context from an existing context.

    if subdir is True it declares a new read-write nested under the old
    context's read-write and adds all read-write and read-only locations
    of the old context as read-only. This is recommended as it makes rolling
    back changes to the global state made in this context easy.

    else the same readwrite/readonly configuration as the parent context is used

    '''
    if oldcontext is None:
        new_readwrite = os.path.abspath(name)
    else:
        new_readwrite = '{}/{}'.format(oldcontext['readwrite'][0],name) if subdir else oldcontext['readwrite'][0]
    if create:
        utils.mkdir_p(new_readwrite)
    newcontext = {
        'nametag':name.replace('/','_'), #replace in case name is nested path
    }
    if subdir:
        readonly =  [ro for ro in itertools.chain(oldcontext['readonly'],oldcontext['readwrite'])] if oldcontext else []
        newcontext.update(readwrite = [new_readwrite], readonly = readonly)
    else:
        readonly = oldcontext['readwrite'] if oldcontext else []
        newcontext.update(readwrite = oldcontext['readwrite'], readonly = oldcontext['readonly'])
    return newcontext

def reset_state(context):
    '''delete readwriteable locations of this context'''
    for rw in context['readwrite']:
        shutil.rmtree(rw)
        os.makedirs(rw)
