import os
import pytest
from jsonschema.exceptions import ValidationError
from packtivity.utils import load_packtivity
from yadageschemas import schemadir


def test_cliload_valid():
    load_packtivity("tests/testspecs/noop-test.yml", os.getcwd(), schemadir, True)


def test_cliload_non_valid():
    with pytest.raises(ValidationError):
        load_packtivity(
            "tests/testspecs/noop-test-invalid.yml", os.getcwd(), schemadir, True
        )


def test_cliload_accept_non_valid():
    load_packtivity(
        "tests/testspecs/noop-test-invalid.yml", os.getcwd(), schemadir, False
    )
