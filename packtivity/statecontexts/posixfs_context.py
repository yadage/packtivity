import hashlib
import os
import shutil
import json
import logging
import checksumdir
import six
import packtivity.utils as utils

log = logging.getLogger(__name__)


class LocalFSState(object):
    """
    Local Filesyste State consisting of a number of readwrite and readonly directories
    """

    def __init__(
        self,
        readwrite=None,
        readonly=None,
        dependencies=None,
        identifier="unidentified_state",
    ):
        try:
            assert type(readwrite) in [list, type(None)]
            assert type(readonly) in [list, type(None)]
        except AssertionError:
            raise TypeError(
                "readwrite and readonly must be None or a list {} {}".format(
                    type(readonly), type(readwrite)
                )
            )
        self._identifier = identifier

        readwrite = readwrite or []

        readonly = readonly or []
        for d in dependencies or []:
            if d.readwrite:  # if dep has readwrite add those
                readonly += d.readwrite
            else:  # else add the readonlies
                readonly += d.readonly

        self._readonly = []
        for i, ro in enumerate(readonly):
            if isinstance(ro, six.string_types):
                name = "readdir{}".format(i)
                self._readonly.append({"path": os.path.realpath(ro), "name": name})
            if isinstance(ro, dict):
                ro["path"] = os.path.realpath(ro["path"])
                self._readonly.append(ro)

        self._readwrite = []
        for i, rw in enumerate(readwrite):
            if isinstance(rw, six.string_types):
                self._readwrite.append({"path": os.path.realpath(rw)})
            if isinstance(rw, dict):
                rw["path"] = os.path.realpath(rw["path"])
                self._readwrite.append(rw)
        if self._readwrite:
            self._readwrite[0]["name"] = "workdir"

        self._readonly = sorted(self._readonly, key=lambda x: x["path"])
        self._readwrite = sorted(self._readwrite, key=lambda x: x["path"])

        self.dirnames = {}
        for x in self._readwrite + self._readonly:
            if "name" in x:
                self.dirnames[x["name"]] = x["path"]

        self.datamodel = None

    def __repr__(self):
        return "<LocalFSState rw: {}, ro: {}>".format(self.readwrite, self.readonly)

    @property
    def readonly(self):
        return [x["path"] for x in self._readonly]

    @property
    def readwrite(self):
        return [x["path"] for x in self._readwrite]

    @property
    def metadir(self):
        if self.readwrite:
            return "{}/_packtivity".format(self.readwrite[0])
        return None

    def identifier(self):
        return self._identifier

    def reset(self):
        """
        resets state by deleting readwrite directory contents (deletes tree and re-creates)
        """
        for rw in self.readwrite + [self.metadir]:
            if rw and os.path.exists(rw):
                shutil.rmtree(rw)
        self.ensure()

    def ensure(self):
        """
        ensures existence of readwrite and meta directories.
        """
        for d in self.readwrite:
            utils.mkdir_p(d)
        utils.mkdir_p(self.metadir)

    def state_hash(self):
        """
        generate hash to snapshot current state (used for caching / change detection)
        checks both readwrite directories and dependencies (assumed to be subtrees of readwrite directories)
        return: SHA1 hash
        """
        # hash the upstream / input state
        dep_checksums = [
            checksumdir.dirhash(d) for d in self.readonly if os.path.isdir(d)
        ]

        # hash out writing state
        state_checksums = [
            checksumdir.dirhash(d) for d in self.readwrite if os.path.isdir(d)
        ]
        return hashlib.sha1(
            json.dumps([dep_checksums, state_checksums]).encode("utf-8")
        ).hexdigest()

    def contextualize_value(self, value):
        """
        contextualizes string data by string interpolation.
        replaces '{workdir}' placeholder with first readwrite directory
        """
        try:
            return value.format(**self.dirnames)
        except AttributeError:
            return value
        except IndexError:
            return value

    def model(self, data):
        data = data.copy()
        for p, v in data.leafs():
            data.replace(p, self.contextualize_value(v))
        return data

    def json(self):
        return {
            "state_type": "localfs",
            "identifier": self.identifier(),
            "readwrite": self._readwrite,
            "readonly": self._readonly,
        }

    @classmethod
    def fromJSON(cls, jsondata):
        return cls(
            identifier=jsondata["identifier"],
            readwrite=jsondata["readwrite"],
            readonly=jsondata["readonly"],
        )
