from packtivity.typedleafs import TypedLeafs


class MyClass(object):
    def __init__(self, first_attr, second_attr):
        self.first_attr = first_attr
        self.second_attr = second_attr

    @classmethod
    def fromJSON(cls, data):
        return cls(**data)

    def json(self):
        return {"first_attr": self.first_attr, "second_attr": self.second_attr}


datamodel = {"keyword": "$type", "types": {"MyClass": MyClass}}

simple_data = {
    "hello": {"$type": "MyClass", "first_attr": "hello", "second_attr": "world"}
}

nested_data = {
    "list_of_things": [
        {"$type": "MyClass", "first_attr": "hello", "second_attr": "world"},
        {"$type": "MyClass", "first_attr": "hello", "second_attr": "world"},
    ],
    "single_thing": {"$type": "MyClass", "first_attr": "hello", "second_attr": "world"},
}


def test_init():
    tl = TypedLeafs(simple_data, datamodel)
    assert type(tl["hello"]) == MyClass
    assert tl["hello"].first_attr == "hello"
    assert tl["hello"].second_attr == "world"
    assert tl.json() == simple_data

    tl = TypedLeafs.fromJSON(simple_data, deserialization_opts={"leafmodel": datamodel})
    assert tl.json() == simple_data


def test_deepnest():
    tl = TypedLeafs(nested_data, datamodel)
    paths = [p.path for p, v in tl.leafs()]
    assert set(paths) == set(
        ["/list_of_things/0", "/list_of_things/1", "/single_thing"]
    )


def test_jq():
    tl = TypedLeafs(nested_data, datamodel)
    assert (
        tl.jq(".list_of_things[]", multiple_output=True)[0].json()
        == tl["list_of_things"][0].json()
    )
    assert (
        tl.jq(".list_of_things[]", multiple_output=True)[1].json()
        == tl["list_of_things"][1].json()
    )

    assert tl.jq("[.list_of_things[]]").json() == nested_data["list_of_things"]


def test_jsonpath():
    tl = TypedLeafs(nested_data, datamodel)

    assert tl.jsonpath("single_thing").json() == tl["single_thing"].json()
    assert (
        tl.jsonpath("list_of_things[*]", multiple_output=True)[0].json()
        == tl["list_of_things"][0].json()
    )


def test_jsonpointer():
    tl = TypedLeafs(nested_data, datamodel)
    for p, v in tl.leafs():
        try:
            assert tl.jsonpointer(p.path).json() == v.json()
        except AttributeError:
            assert tl.jsonpointer(p.path) == v


def test_refs():
    import jq

    refs = TypedLeafs(nested_data, datamodel).asrefs()
    assert refs["list_of_things"][0].path == "/list_of_things/0"

    import jsonpointer

    jp = jsonpointer.JsonPointer("/list_of_things/0")

    tl = TypedLeafs(nested_data, datamodel)
    assert tl.resolve_ref(jp).json() == tl["list_of_things"][0].json()


def test_modify():
    import jq

    tl = TypedLeafs(nested_data, datamodel)
    tlnew = TypedLeafs(
        {"$type": "MyClass", "second_attr": "newsecond", "first_attr": "newfirst"},
        datamodel,
    )

    tl["single_thing"] = tlnew.typed()
    assert type(tlnew.typed()) == MyClass
    assert tl["single_thing"].json() == tlnew.typed().json()
