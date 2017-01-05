import os
import pytest
from jsonschema.exceptions import ValidationError
from packtivity.cli import load_pack
from yadageschemas import schemadir


def test_cliload_valid():
    load_pack('noop-test.yml', os.getcwd(), schemadir, True)

def test_cliload_non_valid():
    with pytest.raises(ValidationError):
        load_pack('noop-test-invalid.yml', os.getcwd(), schemadir, True)

def test_cliload_accept_non_valid():
    load_pack('noop-test-invalid.yml', os.getcwd(), schemadir, False)

def test_fromgithub_load():
    load_pack('madgraph.yml', 'from-github/phenochain', schemadir, True)

def test_jsonpointer_load():
    load_pack('steps.yml#/prepare', 'from-github/higgsmcproduction' , schemadir, True)
