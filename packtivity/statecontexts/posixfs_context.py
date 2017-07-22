import hashlib
import itertools
import os
import shutil
import json
import packtivity.utils as utils
import logging
import checksumdir
log = logging.getLogger(__name__)

class LocalFSState(object):
    def __init__(self,readwrite = None,readonly = None, dependencies = None, identifier = 'unidentified_state'):
        self._identifier = identifier
        self.readwrite = map(os.path.realpath,readwrite) if readwrite else  []
        self.readonly  = map(os.path.realpath,readonly) if readonly else  []
        self.dependencies = dependencies or []
        self.metadir = None

    def identifier(self):
        return self._identifier

    def add_dependency(self,depstate):
        self.dependencies.append(depstate)

    def reset(self):
        for rw in self.readwrite:
            if os.path.exists(rw):
                shutil.rmtree(rw)
            os.makedirs(rw)

    def state_hash(self):
        #hash the upstream / input state
        depwrites = [deprw for dep in self.dependencies for deprw in dep.readwrite]
        dep_checksums = [checksumdir.dirhash(d) for d in depwrites if os.path.isdir(d)]

        #hash out writing state
        state_checksums = [checksumdir.dirhash(d) for d in self.readwrite if os.path.isdir(d)]
        return hashlib.sha1(json.dumps([dep_checksums,state_checksums])).hexdigest()

    def contextualize_data(self,data):
        try: 
            workdir = self.readwrite[0]
            return data.format(workdir = workdir)
        except AttributeError:
            return data


    def json(self):
        return {
            'state_type': 'localfs',
            'identifier': self.identifier(),
            'readwrite':  self.readwrite,
            'readonly':   self.readonly,
            'dependencies': [x.json() for x in self.dependencies]
        }

    @classmethod
    def fromJSON(cls,jsondata):
        return cls(
            readwrite    = jsondata['readwrite'],
            readonly     = jsondata['readonly'],
            identifier   = jsondata['identifier'],
            dependencies = [LocalFSState.fromJSON(x) for x in jsondata['dependencies']]
        )

class LocalFSProvider(object):
    def __init__(self, *base_states, **kwargs):
        base_states = list(base_states)
        self.nest = kwargs.get('nest', True)
        self.ensure = kwargs.get('ensure', None)

        first = base_states.pop()
        assert first

        self.base = first

        while base_states:
            next_state = base_states.pop()
            if not next_state:
                continue
            self.base = _merge_states(self.base,next_state)

    def new_provider(self,name):

        new_base_ro = self.base.readwrite + self.base.readonly
        new_base_rw = [os.path.join(self.base.readwrite[0],name)]
        return LocalFSProvider(LocalFSState(new_base_rw,new_base_ro))

    def new_state(self,name):
        return _make_new_state(name,self.base, self.nest, self.ensure)

    def json(self):
        return {
            'state_provider_type': 'localfs_provider',
            'base_state': self.base.json(),
            'nest': self.nest,
            'ensure': self.ensure
        }

    @classmethod
    def fromJSON(cls,jsondata):
        return cls(LocalFSState.fromJSON(jsondata['base_state']), nest = jsondata['nest'], ensure = jsondata['ensure'])

def _merge_states(lhs,rhs):
    return LocalFSState(lhs.readwrite + rhs.readwrite,lhs.readonly + rhs.readonly)

def _make_new_state(name, oldstate = None, subdir = True, create = False):
    '''
    creates a new context from an existing context.

    if subdir is True it declares a new read-write nested under the old
    context's read-write and adds all read-write and read-only locations
    of the old context as read-only. This is recommended as it makes rolling
    back changes to the global state made in this context easy.

    else the same readwrite/readonly configuration as the parent context is used

    '''

    # the new context will get a name in any case (if subdir is false someone needs to make sure these are unique)
    if 'PACKTIVITY_FORCESHAREDSTATE' in os.environ:
        subdir = False

    if oldstate is None:
        new_readwrites = [os.path.abspath(name)]
    else:
        new_readwrites = ['{}/{}'.format(oldstate.readwrite[0],name)] if subdir else oldstate.readwrite

    if subdir:
        # for nested directories, we want to have at lease read access to all data in parent context
        new_readonlies = [ro for ro in itertools.chain(oldstate.readonly,oldstate.readwrite)] if oldstate else []
    else:
        new_readonlies = oldstate.readonly if oldstate else []
        
    if create:
        map(utils.mkdir_p,new_readwrites)
        
    log.debug('new context is: rw: %s, ro: ', new_readwrites, new_readonlies)
    new_identifier = name.replace('/','_') # replace in case name is nested path
    return LocalFSState(readwrite = new_readwrites, readonly = new_readonlies, identifier = new_identifier)

