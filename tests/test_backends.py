import pytest
from packtivity.utils import backend_from_string

def test_multiproc_fixed_ncpu_auto():
    backend_from_string('multiproc:auto')

def test_multiproc_fixed_ncpu_specific():
    backend_from_string('multiproc:4')

def test_celery():
    backend_from_string('celery')

# def test_ipcluster():
#     backend_from_string('ipcluster')


def test_unknown():
    with pytest.raises(RuntimeError):
        backend_from_string('doesnotexist')
