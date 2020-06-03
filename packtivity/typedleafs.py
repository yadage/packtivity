import json
import jq
import jsonpointer
import jsonpath_rw
import copy
import base64
import importlib
from six import string_types
import logging

log = logging.getLogger(__name__)


class LeafModel(object):
    def __init__(self, spec):
        self.datamodel = spec or {"keyword": None, "types": {}}
        self._types2str, self._str2types = {}, {}
        for name, class_def in list(self.datamodel["types"].items()):
            if type(class_def) == type:
                self._types2str[class_def] = name
                self._str2types[name] = class_def
            elif isinstance(class_def, string_types):
                m, c = class_def.split(":")
                c = getattr(importlib.import_module(m), c)
                self._types2str[c] = name
                self._str2types[name] = c
            else:
                raise RuntimeError("not sure how to interpret type def %s", class_def)
        self.keyword = self.datamodel["keyword"]
        self.canonical_leaf_magic = "b64json://"
        lits = self.datamodel.get("literals")

        self.magics = [self.canonical_leaf_magic]

        if lits:
            m, f = lits["parser"].split(":")
            self.litparser = getattr(importlib.import_module(m), f)
            self.magics += lits["magics"]

    def leaf_encode(self, obj):
        return self.canonical_leaf_magic + base64.b64encode(
            json.dumps(self.dumper(obj)).encode("utf-8")
        ).decode("utf-8")

    def leaf_decode(self, encoded):
        for m in self.magics:
            if encoded.startswith(m):
                if m == self.canonical_leaf_magic:
                    magic_replaced = encoded.replace(self.canonical_leaf_magic, "")
                    return json.loads(base64.b64decode(magic_replaced).decode("utf-8"))
                else:
                    return self.litparser(encoded)
        raise RuntimeError("cannot decode {} ".format(encoded))

    def loader(self, spec, idleafs):
        if not self.keyword:
            return spec

        found_identifiers = set([self.keyword]).intersection(set(spec.keys()))
        found_identifiers = {k: spec[k] for k in found_identifiers}
        if not found_identifiers:
            return spec

        for k in list(found_identifiers.keys()):
            spec.pop(k)
        cl = self._str2types[found_identifiers[self.keyword]]
        obj = cl.fromJSON(spec)
        if not idleafs:
            return obj
        return self.leaf_encode(obj)

    def dumper(self, obj):
        json = obj.json()
        if not type(obj) == TypedLeafs:
            try:
                json[self.keyword] = self._types2str[type(obj)]
            except KeyError:
                log.exception("could not find type in %s", self._types2str)
                raise
        return json


class TypedLeafs(object):
    def __init__(self, data, leafmodel=None, idleafs=False):
        self.leafmodel = leafmodel
        self._leafmodel = LeafModel(leafmodel)

        if isinstance(data, TypedLeafs):
            data = data.json()
        self._jsonable = data

    def __repr__(self):
        return "<TypedLeafs: {}>".format(self.typed())

    def __getitem__(self, key):
        return self.typed().__getitem__(key)

    def __iter__(self):
        return self.typed().__iter__()

    def __len__(self):
        return self.typed().__len__()

    def __delitem__(self, key):
        self._jsonable.__delitem__(key)

    def __setitem__(self, key, value):
        data = self._jsonable
        data.__setitem__(key, value)
        self._jsonable = data

    def __normalize(self, idleafs=True):
        # wrap in a simple dict, necessary for if data is just a leaf value
        data = {
            "data": self._load_from_string(
                self._dump_to_string(self._jsonable), typed=False
            )
        }
        if idleafs:
            magicexpr = " or ".join(
                ['startswith("{}")'.format(m) for m in self._leafmodel.magics]
            )
            ptrs = [
                jsonpointer.JsonPointer.from_parts(x)
                for x in jq.jq(
                    'paths(type=="string" and ({}))'.format(magicexpr)
                ).transform(data, multiple_output=True)
            ]
            for p in ptrs:
                p.set(data, self._leafmodel.leaf_decode(p.get(data)))
        self.__jsonable = data["data"]

    @property
    def _jsonable(self):
        return self.__jsonable

    @_jsonable.setter
    def _jsonable(self, value):
        pass
        self.__jsonable = value
        self.__normalize()

    @classmethod
    def fromJSON(cls, data, deserialization_opts):
        return cls(
            data,
            deserialization_opts.get("leafmodel", None),
            deserialization_opts.get("idleafs", False),
        )

    def _load_from_string(self, jsonstring, typed=True, idleafs=False):
        if typed:
            data = json.loads(
                jsonstring,
                object_hook=lambda spec: self._leafmodel.loader(spec, idleafs),
            )
            return data
        else:
            return json.loads(jsonstring)

    def _dump_to_string(self, data):
        return json.dumps(data, default=self._leafmodel.dumper)

    def replace(self, path, value):
        self._jsonable = TypedLeafs(
            path.set(self.json(), value, inplace=False), self.leafmodel
        ).json()

    ### representation methods
    def json(self):
        return self._jsonable

    def typed(self, idleafs=False):
        return self._load_from_string(
            json.dumps(self._jsonable, sort_keys=True), typed=True, idleafs=idleafs
        )

    def copy(self):
        return TypedLeafs(copy.deepcopy(self.typed()), self.leafmodel)

    def asrefs(self, callback=None):
        data = self.copy().json()
        for p, v in self.leafs():
            if p.path == "":
                return p if not callback else callback(p)
            p.set(data, p if not callback else callback(p))
        return data

    ### QUERY methods
    def resolve_ref(self, reference):
        return reference.get(self.typed())

    def jsonpointer(self, pointer_str):
        return jsonpointer.JsonPointer(pointer_str).resolve(self.typed())

    def jsonpath(self, jsonpath_expression, multiple_output=False):
        if not multiple_output:
            return jsonpath_rw.parse(jsonpath_expression).find(self.typed())[0].value
        else:
            return [
                x.value
                for x in jsonpath_rw.parse(jsonpath_expression).find(self.typed())
            ]

    def jq(self, jq_program, *args, **kwargs):
        return TypedLeafs(
            jq.jq(jq_program).transform(self.typed(idleafs=True), *args, **kwargs),
            self.leafmodel,
            idleafs=True,
        )

    def leafs(self):
        if not isinstance(self.typed(), (list, dict)):
            yield jsonpointer.JsonPointer(""), self.typed()
        else:
            ptrs = [
                jsonpointer.JsonPointer.from_parts(parts)
                for parts in self.jq("leaf_paths", multiple_output=True).typed()
            ]
            for p in ptrs:
                yield p, p.get(self.typed())
