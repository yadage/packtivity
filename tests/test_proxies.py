from packtivity.backendutils import load_proxy


def test_celery():
    from packtivity.asyncbackends import CeleryProxy
    from celery.result import AsyncResult

    asyncresult = AsyncResult("1234")
    p = CeleryProxy(asyncresult)

    p, _ = load_proxy(p.json())
    assert type(p) == CeleryProxy

    p.details()["task_id"] == asyncresult.task_id


def test_foreground():
    from packtivity.asyncbackends import ForegroundProxy

    p = ForegroundProxy({"hello": "world"}, None, True)
    p, _ = load_proxy(p.json())
    assert type(p) == ForegroundProxy


def test_python():
    from packtivity.asyncbackends import ForegroundProxy

    p = ForegroundProxy({"hello": "world"}, None, True)
    p = load_proxy(
        p.json(),
        {"proxy": "py:packtivity.asyncbackends:ForegroundProxy"},
        best_effort_backend=False,
    )
    assert type(p) == ForegroundProxy


def test_env(monkeypatch):
    from packtivity.asyncbackends import ForegroundProxy

    monkeypatch.setenv(
        "PACKTIVITY_ASYNCBACKEND",
        "packtivity.asyncbackends:ForegroundBackend:ForegroundProxy",
    )

    p = ForegroundProxy({"hello": "world"}, None, True)
    p, _ = load_proxy(p.json())
    assert type(p) == ForegroundProxy
