import json
import jq
import jsonpointer
import jsonpath_rw
import copy
import base64
import importlib
import collections

class LeafModel(object):
    def __init__(self, spec):
        self.datamodel = spec or {'keyword': None, 'types': {}}
        self._types2str, self._str2types = {}, {}
        for name,module_class in self.datamodel['types'].items():
            m, c = module_class.split(':')
            c = getattr(importlib.import_module(m),c)
            self._types2str[c] = name
            self._str2types[name] = c
        self.keyword = self.datamodel['keyword']
        self.leaf_magic = '___leaf___'

    def leaf_encode(self,obj):
        return self.leaf_magic + base64.b64encode(json.dumps(self.dumper(obj)))

    @staticmethod
    def leaf_decode(str):
        return json.loads(base64.b64decode(str))

    def loader(self, spec, idleafs):
        if not self.keyword: return spec

        found_identifiers = set([self.keyword]).intersection(set(spec.keys()))
        found_identifiers = {k:spec[k] for k in found_identifiers}
        if not found_identifiers: return spec

        for k in found_identifiers.keys():
            spec.pop(k)
        cl  = self._str2types[found_identifiers[self.keyword]]
        obj =  cl.fromJSON(spec)
        if not idleafs:
            return obj
        return self.leaf_encode(obj)

    def dumper(self, obj):
        json = obj.json()
        if not type(obj)==TypedLeafs:
            json[self.keyword] = self._types2str[type(obj)]
        return json

class TypedLeafs(collections.MutableMapping):
    def __init__(self,data, leafmodel = None, idleafs = False):
        self.leafmodel = leafmodel
        self._leafmodel = LeafModel(leafmodel)


        if isinstance(data, TypedLeafs):
            data = data.json()
        self._jsonable = data

    def __getitem__(self,key):
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

    def __normalize(self,idleafs = True):
        #wrap in a simple dict, necessary for if data is just a leaf value
        data =  {'data': self._load_from_string(self._dump_to_string(self._jsonable), typed=False)}
        if idleafs:
            ptrs = [jsonpointer.JsonPointer.from_parts(x) for x in jq.jq('paths(type=="string" and startswith("{}"))'.format(self._leafmodel.leaf_magic)).transform(data, multiple_output = True)]
            for p in ptrs:
                p.set(data,self._leafmodel.leaf_decode(p.get(data).replace('___leaf___','')))
        self.__jsonable = data['data']

    @property
    def _jsonable(self):
        return self.__jsonable

    @_jsonable.setter
    def _jsonable(self, value):
        pass
        self.__jsonable = value
        self.__normalize()


    @classmethod
    def fromJSON(cls, deserialization_opts):
        return cls(data, deserialization_opts['leafmodel'], deserialization_opts['idleafs'])

    def _load_from_string(self,jsonstring, typed = True, idleafs = False):
        if typed:
            data = json.loads(jsonstring, object_hook = lambda spec: self._leafmodel.loader(spec, idleafs))
            return data
        else:
            return json.loads(jsonstring)

    def _dump_to_string(self,data):
        return json.dumps(data, default = self._leafmodel.dumper)


    ### representation methods
    def json(self):
        return self._jsonable

    def typed(self, idleafs = False):
        return self._load_from_string(json.dumps(self._jsonable, sort_keys = True), typed=True, idleafs = idleafs)

    def copy(self):
        return TypedLeafs(copy.deepcopy(self.typed()), self.leafmodel)

    def asrefs(self):
        data = copy.deepcopy(self.typed())
        for p, v in self.leafs():
            p.set(data, p)
        return data

    ### QUERY methods
    def resolve_ref(self, reference):
        return reference.resolve(self.typed())

    def jsonpointer(self,pointer_str):
        return jsonpointer.JsonPointer(pointer_str).resolve( self.typed() )

    def jsonpath(self,jsonpath_expression):
        return jsonpath_rw.parse(jsonpath_expression).find( self.typed() )[0].value

    def jq(self,jq_program, *args, **kwargs):
        return TypedLeafs(jq.jq(jq_program).transform(self.typed(idleafs = True), *args, **kwargs), self.leafmodel, idleafs = True)

    def leafs(self):
        ptrs = [jsonpointer.JsonPointer.from_parts(parts) for parts in self.jq('leaf_paths', multiple_output = True).typed()]
        for p in ptrs:
            yield p, p.get(self.typed())