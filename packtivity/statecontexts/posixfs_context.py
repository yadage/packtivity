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
    def __init__(self,readwrite = None, readonly = None, dependencies = None, identifier = 'unidentified_state'):
        try:
            assert type(readwrite) in [list, type(None)]
            assert type(readonly) in [list, type(None)]
        except AssertionError:
            raise TypeError('readwrite and readonly must be None or a list {} {}'.format(type(readonly)))
        self._identifier = identifier

        readonlies = list(map(os.path.realpath,readonly) if readonly else  [])
        for d in dependencies or []:
            if d.readwrite:
                readonlies += d.readwrite # if dep has readwrite add those
            else:
                readonlies += d.readonly # else add the readonlies

        self.readwrite = sorted(list(map(os.path.realpath,readwrite) if readwrite else  []))
        self.readonly  = sorted(list(map(os.path.realpath,readonlies)))

        # self.aliases = {
        #     'workdir': self.readonly[0]
        # }

        self.datamodel = None

    def __repr__(self):
        return '<LocalFSState rw: {}, ro: {}>'.format(self.readwrite,self.readonly)


    @property
    def metadir(self):
        if self.readwrite:
            return '{}/_packtivity'.format(self.readwrite[0])
        return None

    def identifier(self):
        return self._identifier

    def reset(self):
        '''
        resets state by deleting readwrite directory contents (deletes tree and re-creates)
        '''
        for rw in self.readwrite + [self.metadir]:
            if rw and os.path.exists(rw):
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
        dep_checksums = [checksumdir.dirhash(d) for d in self.readonly if os.path.isdir(d)]

        #hash out writing state
        state_checksums = [checksumdir.dirhash(d) for d in self.readwrite if os.path.isdir(d)]
        return hashlib.sha1(json.dumps([dep_checksums,state_checksums]).encode('utf-8')).hexdigest()

    def contextualize_value(self,value):
        '''
        contextualizes string data by string interpolation.
        replaces '{workdir}' placeholder with first readwrite directory
        '''
        try:
            workdir = self.readwrite[0]
            return value.format(workdir = workdir)
        except AttributeError:
            return value
        except IndexError:
            return value

    def model(self, data):
        data = data.copy()
        for p, v in data.leafs():
            data.replace(p,self.contextualize_value(v))
        return data

    def json(self):
        return {
            'state_type': 'localfs',
            'identifier': self.identifier(),
            'readwrite':  self.readwrite,
            'readonly':   self.readonly,
        }

    @classmethod
    def fromJSON(cls,jsondata):
        return cls(
            identifier   = jsondata['identifier'],
            readwrite    = sorted(jsondata['readwrite']),
            readonly     = sorted(jsondata['readonly']),
        )
