import pytest
from packtivity.backendutils import backend_from_string


def test_known():
    for known_backend in [
        "celery",
        "multiproc:4",
        "multiproc:auto",
        "foregroundasync",
        "externalasync:default",
    ]:
        b = backend_from_string(known_backend)
        assert b


def test_python_import():
    b = backend_from_string(
        "py:packtivity.asyncbackends:MultiProcBackend", {"poolsize": 1}
    )
    assert b


def test_env_import(tmpdir, monkeypatch):
    monkeypatch.setenv(
        "PACKTIVITY_ASYNCBACKEND",
        "packtivity.asyncbackends:ForegroundBackend:ForegroundProxy",
    )
    b = backend_from_string("fromenv")
    optfile = tmpdir.join("opt.yml")
    optfile.write("{}")
    monkeypatch.setenv("PACKTIVITY_ASYNCBACKEND_OPTS", str(optfile))
    b = backend_from_string("fromenv")
    assert b


def test_unknown():
    with pytest.raises(RuntimeError):
        backend_from_string("doesnotexist")
