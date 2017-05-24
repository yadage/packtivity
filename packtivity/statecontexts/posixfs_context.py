import itertools
import os
import shutil
import packtivity.utils as utils
import logging

log = logging.getLogger(__name__)

def contextualize_data(data,context):
    '''
    interpolate data string with context-specific information.
    here, replace {workdir} with first read-write location
    '''
    try: 
        workdir = context['readwrite'][0]
        return data.format(workdir = workdir)
    except AttributeError:
        return data

def merge_contexts(lhs,rhs):
    return {
        'readonly': lhs.get('readonly',[]) + rhs.get('readonly',[]),
        'readwrite': lhs.get('readwrite',[]) + rhs.get('readwrite',[])
    }

def make_new_context(name, oldcontext = None, subdir = True, create = False):
    '''
    creates a new context from an existing context.

    if subdir is True it declares a new read-write nested under the old
    context's read-write and adds all read-write and read-only locations
    of the old context as read-only. This is recommended as it makes rolling
    back changes to the global state made in this context easy.

    else the same readwrite/readonly configuration as the parent context is used

    '''

    # the new context will get a name in any case (if subdir is false someone needs to make sure these are unique)
    newcontext = {
        'nametag':name.replace('/','_'), # replace in case name is nested path
    }

    if 'PACKTIVITY_FORCESHAREDSTATE' in os.environ:
        subdir = False

    if oldcontext is None:
        new_readwrites = [os.path.abspath(name)]
    else:
        new_readwrites = ['{}/{}'.format(oldcontext['readwrite'][0],name)] if subdir else oldcontext['readwrite'] 

    if subdir:
        # for nested directories, we want to have at lease read access to all data in parent context
        new_readonlies = [ro for ro in itertools.chain(oldcontext['readonly'],oldcontext['readwrite'])] if oldcontext else []
    else:
        new_readonlies = oldcontext['readonly'] if oldcontext else []
        
    if create:
        map(utils.mkdir_p,new_readwrites)
        
        
    newcontext.update(readwrite = new_readwrites, readonly = new_readonlies)
    log.debug('new context is: %s', newcontext)
    return newcontext

def reset_state(context):
    '''delete readwriteable locations of this context'''
    for rw in context['readwrite']:
        shutil.rmtree(rw)
        os.makedirs(rw)
