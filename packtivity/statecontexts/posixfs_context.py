import hashlib
import itertools
import os
import shutil
import json
import logging
import checksumdir

import packtivity.utils as utils
log = logging.getLogger(__name__)

class LocalFSState(object):
    '''
    Local Filesyste State consisting of a number of readwrite and readonly directories
    '''
    def __init__(self,readwrite = None,readonly = None, dependencies = None, identifier = 'unidentified_state'):
        try:
            assert type(readwrite) in [list, type(None)]
            assert type(readonly) in [list, type(None)]
        except AssertionError:
            raise TypeError('readwrite and readonly must be None or a list {} {}'.format(type(readwrite), type(readonly)))
        self._identifier = identifier
        self.readwrite = list(map(os.path.realpath,readwrite) if readwrite else  [])
        self.readonly  = list(map(os.path.realpath,readonly) if readonly else  [])
        self.dependencies = dependencies or []

    @property
    def metadir(self):
        return '{}/_packtivity'.format(self.readwrite[0])

    def identifier(self):
        return self._identifier

    def add_dependency(self,depstate):
        self.dependencies.append(depstate)

    def reset(self):
        '''
        resets state by deleting readwrite directory contents (deletes tree and re-creates)
        '''
        for rw in self.readwrite:
            if os.path.exists(rw):
                shutil.rmtree(rw)
        self.ensure()

    def ensure(self):
        '''
        ensures existence of readwrite and meta directories.
        '''
        for d in self.readwrite:
            utils.mkdir_p(d)
        utils.mkdir_p(self.metadir)

    def state_hash(self):
        '''
        generate hash to snapshot current state (used for caching / change detection)
        checks both readwrite directories and dependencies (assumed to be subtrees of readwrite directories)
        return: SHA1 hash
        '''

        #hash the upstream / input state
        depwrites = [deprw for dep in self.dependencies for deprw in dep.readwrite]
        dep_checksums = [checksumdir.dirhash(d) for d in depwrites if os.path.isdir(d)]

        #hash out writing state
        state_checksums = [checksumdir.dirhash(d) for d in self.readwrite if os.path.isdir(d)]
        return hashlib.sha1(json.dumps([dep_checksums,state_checksums]).encode('utf-8')).hexdigest()

    def contextualize_data(self,data):
        '''
        contextualizes string data by string interpolation.
        replaces '{workdir}' placeholder with first readwrite directory
        '''
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

def _merge_states(lhs,rhs):
    return LocalFSState(lhs.readwrite + rhs.readwrite,lhs.readonly + rhs.readonly)

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
        return LocalFSProvider(LocalFSState(new_base_rw,new_base_ro), nest = self.nest, ensure = self.ensure)


    def new_state(self,name):
        '''
        creates a new context from an existing context.

        if subdir is True it declares a new read-write nested under the old
        context's read-write and adds all read-write and read-only locations
        of the old context as read-only. This is recommended as it makes rolling
        back changes to the global state made in this context easy.

        else the same readwrite/readonly configuration as the parent context is used

        '''

        if self.base is None:
            new_readwrites = [os.path.abspath(name)]
        else:
            new_readwrites = ['{}/{}'.format(self.base.readwrite[0],name)] if self.nest else self.base.readwrite

        if self.nest:
            # for nested directories, we want to have at lease read access to all data in parent context
            new_readonlies = [ro for ro in itertools.chain(self.base.readonly,self.base.readwrite)] if self.base else []
        else:
            new_readonlies = self.base.readonly if self.base else []

        log.debug('new context is: rw: %s, ro: ', new_readwrites, new_readonlies)
        new_identifier = name.replace('/','_') # replace in case name is nested path
        newstate = LocalFSState(readwrite = new_readwrites, readonly = new_readonlies, identifier = new_identifier)

        if self.ensure:
            newstate.ensure()
        return newstate

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
