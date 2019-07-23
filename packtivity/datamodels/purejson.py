import copy
import jq
import jsonpointer


class PureJsonModel(object):
    def __repr__(self):
        return "<JSON {}>".format(self.data)

    def __init__(self, data, modelspec=None):
        if isinstance(data, PureJsonModel):
            data = data.json()
        self.data = data

    def __getitem__(self, key):
        return self.data.__getitem__(key)

    def __iter__(self):
        return self.data.__iter__()

    def __len__(self):
        return self.data.__len__()

    def __delitem__(self, key):
        self.data.__delitem__(key)

    def __setitem__(self, key, value):
        data = self.data
        data.__setitem__(key, value)
        self.data = data

    def json(self):
        return self.data

    def copy(self):
        return copy.deepcopy(self)

    def typed(self):
        return self.data

    def resolve_ref(self, reference):
        return reference.get(self.typed())

    def jq(self, jq_program, *args, **kwargs):
        return PureJsonModel(jq.jq(jq_program).transform(self.json(), *args, **kwargs))

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

    def replace(self, path, value):
        self.data = path.set(self.json(), value, inplace=True)

    def asrefs(self, callback=None):
        data = self.copy().json()
        for p, v in self.leafs():
            if p.path == "":
                return p if not callback else callback(p)
            p.set(data, p if not callback else callback(p))
        return data
