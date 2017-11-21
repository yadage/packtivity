import hashlib
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

    def __repr__(self):
        return '<LocalFSState rw: {}, ro: {}>'.format(self.readwrite,self.readonly)

    @property
    def metadir(self):
        if self.readwrite:
            return '{}/_packtivity'.format(self.readwrite[0])
        return None

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
        except IndexError:
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
