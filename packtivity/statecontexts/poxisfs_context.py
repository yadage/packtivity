import itertools
import os
import shutil
import packtivity.utils as utils

def make_new_context(name,oldcontext = None):
    '''
    creates a new context from an existing context.
    specifically it declares a new read-write nested under the old
    context's read-write and adds all read-write and read-only locations
    of the old context as read-only
    '''

    if not oldcontext:
        new_readwrite = os.path.abspath(name)
    else:
        new_readwrite = '{}/{}'.format(oldcontext['readwrite'][0],name)
    utils.mkdir_p(new_readwrite)

    newcontext = {
        'nametag':name.replace('/','_'), #replace in case name is nested path
        'readwrite':[new_readwrite],
        'readonly':[]
    }
    if oldcontext:
        newcontext['readonly'] += [ro for ro in itertools.chain(oldcontext['readonly'],oldcontext['readwrite'])]
    return newcontext

def reset_state(context):
    '''delete readwriteable locations of this context'''
    for rw in context['readwrite']:
        shutil.rmtree(rw)
        os.makedirs(rw)
