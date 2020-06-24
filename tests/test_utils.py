import os
import pytest
from packtivity.utils import mkdir_p, leaf_iterator


def test_mkdir_notexist(tmpdir):
    pathtomake = tmpdir.join("hello")
    mkdir_p(str(pathtomake))
    assert pathtomake.check()


def test_mkdir_exist(tmpdir):
    pathtomake = tmpdir.join("hello")
    pathtomake.ensure(dir=True)
    mkdir_p(str(pathtomake))
    assert pathtomake.check()


def test_mkdir_exist_butfile(tmpdir):
    pathtomake = tmpdir.join("hello")
    pathtomake.ensure(file=True)
    with pytest.raises(OSError):
        mkdir_p(str(pathtomake))


def test_leafit():
    testdata = {
        "hello": "world",
        "deeply": {"nested": ["l", "i"], "numbers": 123},
        "bool": True,
    }
    leafs = set([(x.path, y) for x, y in leaf_iterator(testdata)])
    assert leafs == {
        ("/deeply/nested/0", "l"),
        ("/deeply/nested/1", "i"),
        ("/deeply/numbers", 123),
        ("/hello", "world"),
        ("/bool", True),
    }
